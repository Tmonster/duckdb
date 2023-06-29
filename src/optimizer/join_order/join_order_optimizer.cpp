#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>
#include <cmath>
#include "iostream"

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(const unordered_set<T> &a, const unordered_set<T> &b) {
	return std::all_of(a.begin(), a.end(), [&b](typename std::unordered_set<T>::const_reference entry) {
		return b.find(entry) == b.end();
	});
}

void JoinOrderOptimizer::GetColumnBinding(Expression &expression, ColumnBinding &binding) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		// Add to the table index, later when we add the columns to the relational mapping for the
		// cardinality estimator, we will grab the relation_id using relation_mapping[table_index]
		binding = ColumnBinding(colref.binding.table_index, colref.binding.column_index);
	}
	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) { GetColumnBinding(expr, binding); });
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
		// we don't, we need to create one
		auto filter = make_uniq<LogicalFilter>();
		filter->children.push_back(std::move(node));
		node = std::move(filter);
	}
	// push the filter into the LogicalFilter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = node->Cast<LogicalFilter>();
	filter.expressions.push_back(std::move(expr));
	return node;
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == &rel.op) {
			// found it! take ownership of it from the parent
			auto result = std::move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	D_ASSERT(filters.empty() && relations.empty()); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();

	// now we begin optimizing the current plan
	// Skip all operators until we find the first projection, we do this because the HAVING clause inserts a Filter AFTER the
	// group by and this filter cannot be reordered
	// Then we extract a list of all relations that have to be joined together
	// and a list of all conditions/join filters that are applied to them
	vector<reference<LogicalOperator>> filter_operators;
	// Here we need a parent op for some reason. Makes no sense to me.
	if (!relation_extractor.ExtractJoinRelations(*op, filter_operators)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	expression_set_t filter_set;

	// Inspect filter operations so we can create edges in our query graph.
	for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
		if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    f_op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &join = f_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.join_type == JoinType::INNER);
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					filters.push_back(std::move(comparison));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : f_op.expressions) {
				if (filter_set.find(*expression) == filter_set.end()) {
					filter_set.insert(*expression);
					filters.push_back(std::move(expression));
				}
			}
			f_op.expressions.clear();
		}
	}
	// create potential edges from the comparisons
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		// first extract the relation set for the entire filter
		unordered_set<idx_t> relations;
		relation_extractor.ExtractRelationBindings(*filter, relations);
		auto &set = set_manager.GetJoinRelation(relations);
		auto info = make_uniq<FilterInfo>(set, i);

		auto filter_info = info.get();
		filter_infos.push_back(std::move(info));

		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();
			// extract the relation_ids that are required for the left and right side of the comparison
			unordered_set<idx_t> left_relations, right_relations;
			relation_extractor.ExtractRelationBindings(*comparison.left, left_relations);
			relation_extractor.ExtractRelationBindings(*comparison.right, right_relations);
			// Get Column Bindings to know exactly what columns between relations are being joined with each other
			// In the function GetColumnBindings we can add debug info telling us exactly what filters we have gathered.
			GetColumnBinding(*comparison.left, filter_info->left_binding);
			GetColumnBinding(*comparison.right, filter_info->right_binding);
			filter_info->left_join_column = GetFilterString(left_relations, comparison.left->ToString());
			filter_info->right_join_column = GetFilterString(right_relations, comparison.right->ToString());
			if (!left_relations.empty() && !right_relations.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = &set_manager.GetJoinRelation(left_relations);
				filter_info->right_set = &set_manager.GetJoinRelation(right_relations);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_relations, right_relations)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						join_enumerator.query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set,
						                                       filter_info);
						join_enumerator.query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set,
						                                       filter_info);
					} else {
						continue;
					}
					continue;
				}
			}
		} else {
			// potentially a single node filter (i.e table.a > 100)
			// populate left_join_column for future debugging
			filter_info->left_join_column = filter->ToString();
		}
	}

	// the cardinality estimator initializes the leaf join nodes with estimated cardinalities based on
	// certain table filters.
	auto node_ops = cardinality_estimator.InitCardinalityEstimatorProps();

	// now use dynamic programming to figure out the optimal join order
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree
	// NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	for (auto &node_op : node_ops) {
		D_ASSERT(node_op.node);
		join_enumerator.plans[&node_op.node->set] = std::move(node_op.node);
	}

	// now we perform the actual dynamic programming to compute the final result
	join_enumerator.SolveJoinOrder();

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < relations.size(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = set_manager.GetJoinRelation(bindings);
	auto final_plan = join_enumerator.plans.find(&total_relation);
	if (final_plan == join_enumerator.plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (context.config.force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		join_enumerator.GenerateCrossProducts();
		//! solve the join order again
		join_enumerator.SolveJoinOrder();
		// now we can obtain the final plan!
		final_plan = join_enumerator.plans.find(&total_relation);
		D_ASSERT(final_plan != join_enumerator.plans.end());
	}
	// now perform the actual reordering
	return RewritePlan(std::move(plan), *final_plan->second);
}

GenerateJoinRelation JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                       JoinNode &node) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node.left && node.right && node.info) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, *node.left);
		auto right = GenerateJoins(extracted_relations, *node.right);

		if (node.info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
		} else {
			// we have filters, create a join node
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));
			// set the join conditions from the join node
			for (auto &filter_ref : node.info->filters) {
				auto &f = filter_ref.get();
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters[f.filter_index]);
				auto condition = std::move(filters[f.filter_index]);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(left.set, *f.left_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.right_set)) ||
				         (JoinRelationSet::IsSubset(left.set, *f.right_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = condition->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(left.set, *f.left_set);
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(cond.comparison);
				}
				join->conditions.push_back(std::move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = &left.set;
		right_node = &right.set;
		result_relation = &set_manager.Union(*left_node, *right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node.set.count == 1);
		D_ASSERT(extracted_relations[node.set.relations[0]]);
		result_relation = &node.set;
		result_operator = std::move(extracted_relations[node.set.relations[0]]);
	}
	result_operator->estimated_props = node.estimated_props->Copy();
	result_operator->estimated_cardinality = result_operator->estimated_props->GetCardinality<idx_t>();
	result_operator->has_estimated_cardinality = true;
	if (result_operator->type == LogicalOperatorType::LOGICAL_FILTER &&
	    result_operator->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		// FILTER on top of GET, add estimated properties to both
		auto &filter_props = *result_operator->estimated_props;
		auto &child_operator = *result_operator->children[0];
		child_operator.estimated_props = make_uniq<EstimatedProperties>(filter_props.GetCardinality<double>() /
		                                                                    CardinalityEstimator::DEFAULT_SELECTIVITY,
		                                                                filter_props.GetCost<double>());
		child_operator.estimated_cardinality = child_operator.estimated_props->GetCardinality<idx_t>();
		child_operator.has_estimated_cardinality = true;
	}
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filter_infos) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (filters[info.filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info.set.count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto filter = std::move(filters[info.filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info.left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(*left_node, *info.left_set) &&
				    JoinRelationSet::IsSubset(*right_node, *info.right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(*right_node, *info.left_set) &&
				           JoinRelationSet::IsSubset(*left_node, *info.right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = filter->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(std::move(node->children[0]));
					comp_join->children.push_back(std::move(node->children[1]));
					comp_join->conditions.push_back(std::move(cond));
					if (node == result_operator.get()) {
						result_operator = std::move(comp_join);
					} else {
						D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
						result_operator->children[0] = std::move(comp_join);
					}
				} else {
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
					         node->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);
					auto &comp_join = node->Cast<LogicalComparisonJoin>();
					comp_join.conditions.push_back(std::move(cond));
				}
			}
		}
	}
	return GenerateJoinRelation(*result_relation, std::move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	extracted_relations.reserve(relations.size());
	for (auto &relation : relations) {
		extracted_relations.push_back(ExtractJoinRelation(*relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : filters) {
		// check if the filter has already been extracted
		if (filter) {
			// if not we need to push it
			join_tree.op = PushFilter(std::move(join_tree.op), std::move(filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return std::move(join_tree.op);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	       op->type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = std::move(join_tree.op);
	return plan;
}

struct BindingTranslationResult {
	bool found_expression = false;
	bool expression_is_constant = false;
	ColumnBinding new_binding;
};

static BindingTranslationResult RecursiveEnumerateProjectionExpressions(Expression *expr, ColumnBinding &binding) {
	auto ret = BindingTranslationResult();
	ret.new_binding = ColumnBinding(binding.table_index, binding.column_index);
	switch (expr->expression_class) {
	case ExpressionClass::BOUND_FUNCTION: {
		// TODO: Other expression classes that can have 0 children?
		auto &func = expr->Cast<BoundFunctionExpression>();
		// no children some sort of gen_random_uuid() or equivalent.
		if (func.children.size() == 0) {
			ret.found_expression = true;
			ret.expression_is_constant = true;
			return ret;
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		ret.found_expression = true;
		auto &new_col_ref = expr->Cast<BoundColumnRefExpression>();
		ret.new_binding = ColumnBinding(new_col_ref.binding.table_index, new_col_ref.binding.column_index);
		return ret;
	}
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		ret.found_expression = true;
		ret.expression_is_constant = true;
		return ret;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		auto recursive_result = RecursiveEnumerateProjectionExpressions(child.get(), binding);
		if (recursive_result.found_expression) {
			ret = recursive_result;
		}
	});
	// we didn't find a Bound Column Ref
	return ret;
}

static optional_ptr<LogicalOperator> GetDataRetOp(LogicalOperator &op, ColumnBinding &binding) {
	optional_ptr<LogicalOperator> get;
	auto table_index = binding.table_index;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return &op;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_GET: {
		auto table_ids = op.GetTableIndex();
		if (table_ids.size() == 1 && table_ids[0] == binding.table_index) {
			return &op;
		}
		return nullptr;
	}
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto &chunk_get = op.Cast<LogicalColumnDataGet>();
		if (chunk_get.table_index == table_index) {
			return &chunk_get;
		}
		return nullptr;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
		// filter is not a data ret op
		return GetDataRetOp(*op.children.at(0), binding);
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto table_indexes = op.GetTableIndex();
		D_ASSERT(table_indexes.size() > 0);
		auto &proj = op.Cast<LogicalProjection>();
		if (table_indexes[0] != table_index) {
			return nullptr;
		}
		D_ASSERT(proj.expressions.size() > binding.column_index || binding.column_index == DConstants::INVALID_INDEX);
		auto &new_expression = proj.expressions[binding.column_index];
		auto expression_binding_translation = RecursiveEnumerateProjectionExpressions(new_expression.get(), binding);
		if (expression_binding_translation.found_expression && !expression_binding_translation.expression_is_constant) {
			// we have an expression at the binding. If the
			auto ret = GetDataRetOp(*op.children.at(0), expression_binding_translation.new_binding);
			if (ret) {
				binding = expression_binding_translation.new_binding;
				return ret;
			}
			// if the next relation doesn't have a data source operation for the binding
			// chances are the binding was a constant value or some other data generation
			// operator. we just return the projection in this case.
			return &proj;
		}
		if (expression_binding_translation.expression_is_constant) {
			return &proj;
		}
		D_ASSERT(false);
		// no expressions in the projection come from a bound_column_ref
		return &proj;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// We are attempting to get the catalog table for a relation (for statistics/cardinality estimation)
		// any relation here represents a non-reorderable relation from the join plan with at least two children
		// We still want total domain statistics of the columns from the relations that are found within these
		// non-reorderable relations.
		D_ASSERT(table_index != DConstants::INVALID_INDEX);
		auto &left_child = op.children.at(0);
		get = GetDataRetOp(*left_child, binding);
		if (get) {
			return get;
		}
		// test_nested_keys.test has a test where a join has < 2 children.
		D_ASSERT(op.type != LogicalOperatorType::LOGICAL_PROJECTION);
		if (op.children.size() < 2) {
			break;
		}
		auto &right_child = op.children.at(1);
		get = GetDataRetOp(*right_child, binding);
		if (get) {
			return get;
		}
		break;
	}
	// anything with a child that isn't one of the operators above, just pass through.
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = op.Cast<LogicalUnnest>();
		auto table_ids = unnest.GetTableIndex();
		D_ASSERT(table_ids.size() == 1);
		if (table_ids.at(0) != table_index) {
			break;
		}
		if (unnest.children.size() == 0) {
			return nullptr;
		}
		auto &child = unnest.children.at(0);
		auto child_tables = child->GetTableIndex();
		// Go through bound unnest expressions and look for a bound column ref.
		if (child_tables.size() == 0) {
			return GetDataRetOp(*child, binding);
		}
		auto child_table_index = child->GetTableIndex()[0];
		binding = ColumnBinding(child_table_index, binding.column_index);
		for (idx_t column_index = 0; column_index < unnest.expressions.size(); column_index++) {
			auto ret = GetDataRetOp(*child, binding);
			if (ret) {
				return ret;
			}
		}
		break;
	}
	// We are attempting to get the catalog table for a relation (for statistics/cardinality estimation)
	// Logical Aggregates and Group by's are non-reorderable relations
	// We still want total domain statistics of the columns from the relations that are found within these
	// non-reorderable relations.
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto table_indexes = op.GetTableIndex();
		D_ASSERT(table_indexes.size() > 0);
		auto &aggr = op.Cast<LogicalAggregate>();
		if (std::find(table_indexes.begin(), table_indexes.end(), table_index) == table_indexes.end()) {
			return nullptr;
		}
		if (table_index != aggr.group_index) {
			// dataret op is in another operator
			return nullptr;
		}
		auto new_expression = aggr.groups[binding.column_index].get();

		auto expression_binding_translation = RecursiveEnumerateProjectionExpressions(new_expression, binding);
		if (expression_binding_translation.found_expression && !expression_binding_translation.expression_is_constant) {
			// we have an expression at the binding. If the
			auto ret = GetDataRetOp(*op.children.at(0), expression_binding_translation.new_binding);
			if (ret) {
				return ret;
			}
		}
		if (expression_binding_translation.expression_is_constant) {
			return &aggr;
		}
		D_ASSERT(false);
		return &aggr;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return nullptr;
	default:
		if (op.children.size() == 1) {
			return GetDataRetOp(*op.children.at(0), binding);
		}
		D_ASSERT(false);
		// return null pointer, there are no children, so the data source operation is
		// somewhere else in the logical plan.
		break;
	}
	return nullptr;
}

string JoinOrderOptimizer::GetFilterString(unordered_set<idx_t> relation_bindings, string column_name) {
	string ret = "";
#ifdef DEBUG
	for (auto &rel_id : relation_bindings) {
		string table = cardinality_estimator.getRelationAttributes(rel_id).original_name;
		ret += table + "." + column_name + ", ";
	}
#endif
	return ret;
}



} // namespace duckdb
