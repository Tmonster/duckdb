#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_extractor.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <cmath>

namespace duckdb {

void RelationExtractor::GetColumnBinding(Expression &expression, ColumnBinding &binding) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(join_optimizer->relation_mapping.find(colref.binding.table_index) != join_optimizer->relation_mapping.end());
		// Add to the table index, later when we add the columns to the relational mapping for the
		// cardinality estimator, we will grab the relation_id using relation_mapping[table_index]
		binding = ColumnBinding(colref.binding.table_index, colref.binding.column_index);
	}
	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) { GetColumnBinding(expr, binding); });
}


//! Extract the set of relations referred to inside an expression
bool RelationExtractor::ExtractRelationBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(join_optimizer->relation_mapping.find(colref.binding.table_index) !=
		         join_optimizer->relation_mapping.end());
		bindings.insert(join_optimizer->relation_mapping[colref.binding.table_index]);
	}
	if (expression.type == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	D_ASSERT(expression.type != ExpressionType::SUBQUERY);
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
		if (!ExtractRelationBindings(expr, bindings)) {
			can_reorder = false;
			return;
		}
	});
	return can_reorder;
}

string static GetRelationName(optional_ptr<LogicalOperator> op) {
	string ret = op->GetName();
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op->Cast<LogicalGet>();
		if (get.names.size() > 0) {
			ret = get.names.at(0);
		}
		auto catalog_table = get.GetTable();
		if (catalog_table) {
			ret = catalog_table->name;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		ret = "DUMMY_SCAN";
		break;
	}
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto &chunkget = op->Cast<LogicalColumnDataGet>();
		ret = chunkget.collection->ToString();
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		auto left_name = GetRelationName(op->children[0]);
		auto right_name = GetRelationName(op->children[1]);
		ret = left_name + " joined with " + right_name;
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		return GetRelationName(op->children[0]);
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		return GetRelationName(op->children[0]);
	}
	default:
		// should be able to find a relation name
		if (op->children.size() >= 1) {
			return GetRelationName(op->children[0]);
		}
		D_ASSERT(false);
		break;
	}
	return ret;
}

// parent_op <- An operation with two children
// input_op <- The operation that original is the child of parent_op and potentially gets reordered
// data_retreival_op <- the data retreival operation that carries the projected table index & bindings needed for the
// cardinality estimator.
void RelationExtractor::AddRelation(optional_ptr<LogicalOperator> &parent, LogicalOperator &input_op,
                                    LogicalOperator &data_retreival_op) {

	// if parent is null, then this is a root relation
	// if parent is not null, it should have multiple children
	D_ASSERT(!parent || parent->children.size() >= 2);
	auto relation = make_uniq<SingleJoinRelation>(input_op, parent, data_retreival_op);
	auto relation_id = join_optimizer->relations.size();

	auto table_indexes = data_retreival_op.GetTableIndex();
	if (table_indexes.empty()) {
		// relation represents a non-reorderable relation, most likely a join relation
		// Get the tables referenced in the non-reorderable relation and add them to the relation mapping
		// This should all table references, even if there are nested non-reorderable joins.
		unordered_set<idx_t> table_references;
		LogicalJoin::GetTableReferences(data_retreival_op, table_references);
		D_ASSERT(table_references.size() > 0);
		for (auto &reference : table_references) {
			D_ASSERT(join_optimizer->relation_mapping.find(reference) == join_optimizer->relation_mapping.end());
			join_optimizer->relation_mapping[reference] = relation_id;
		}
	} else {
		// Relations should never return more than 1 table index
		D_ASSERT(table_indexes.size() == 1);
		idx_t table_index = table_indexes.at(0);
		D_ASSERT(join_optimizer->relation_mapping.find(table_index) == join_optimizer->relation_mapping.end());
		join_optimizer->relation_mapping[table_index] = relation_id;
	}
	// Add binding information from the nonreorderable join to this relation.
	auto relation_name = GetRelationName(&data_retreival_op);
	// TODO: figure out how to remove this. Relation extractor and cardinality estimator should not
	//       be calling each others functions. Can probably just add every relation during a cardinality_estimator init phase.
	join_optimizer->cardinality_estimator.AddRelationId(relation_id, relation_name);
	join_optimizer->relations.push_back(std::move(relation));
}

bool RelationExtractor::ExtractJoinRelations(LogicalOperator &input_op,
                                             optional_ptr<LogicalOperator> parent) {
	LogicalOperator *op = &input_op;
	while (op->children.size() == 1 &&
	       (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
	        op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET && op->type != LogicalOperatorType::LOGICAL_GET)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			// extract join conditions from filter
			filter_operators.push_back(*op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
		    op->type == LogicalOperatorType::LOGICAL_WINDOW) {
			// don't push filters through projection or aggregate and group by
			JoinOrderOptimizer optimizer(context);
			op->children[0] = optimizer.Optimize(std::move(op->children[0]));
			return false;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	    op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ANY_JOIN || op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(*op);
		} else {
			non_reorderable_operation = true;
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		for (auto &child : op->children) {
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child));
		}
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op->Cast<LogicalComparisonJoin>();
			if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
				// for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
				// we convert to doing a RIGHT OUTER JOIN
				// FIXME: for now we don't swap if the right_projection_map is not empty
				// this can be fixed once we implement the left_projection_map properly...
				auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
				auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
				if (rhs_cardinality > lhs_cardinality * 2) {
					join.join_type = JoinType::RIGHT;
					std::swap(join.children[0], join.children[1]);
					for (auto &cond : join.conditions) {
						std::swap(cond.left, cond.right);
						cond.comparison = FlipComparisonExpression(cond.comparison);
					}
				}
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN && non_reorderable_operation) {
		auto &join = op->Cast<LogicalAnyJoin>();
		if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
			auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
			auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
			if (rhs_cardinality > lhs_cardinality * 2) {
				join.join_type = JoinType::RIGHT;
				std::swap(join.children[0], join.children[1]);
			}
		}
	}

	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		for (auto &child : op->children) {
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child));
		}
		AddRelation(parent, input_op, *op);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations
		AddRelation(parent, input_op, *op);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op->children.empty() && op->type == LogicalOperatorType::LOGICAL_GET) {
			AddRelation(parent, input_op, *op);
			return true;
		}
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]));
		// have to be careful here. projections can sit on joins and have more columns than
		// the original logical get underneath. For this reason we need to copy some bindings from
		// the optimizer just declared, so we know what columns map to
		AddRelation(parent, input_op, *op);
		return true;
	}
	default:
		return false;
	}
}

void RelationExtractor::ExtractFilters() {

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
					join_optimizer->filters.push_back(std::move(comparison));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : f_op.expressions) {
				if (filter_set.find(*expression) == filter_set.end()) {
					filter_set.insert(*expression);
					join_optimizer->filters.push_back(std::move(expression));
				}
			}
			f_op.expressions.clear();
		}
	}
}

string RelationExtractor::GetFilterString(unordered_set<idx_t> relation_bindings, string column_name) {
	string ret = "";
#ifdef DEBUG
	for (auto &rel_id : relation_bindings) {
		string table = join_optimizer->cardinality_estimator.getRelationAttributes(rel_id).original_name;
		ret += table + "." + column_name + ", ";
	}
#endif
	return ret;
}

void RelationExtractor::CreateQueryGraph() {
	for (idx_t i = 0; i < join_optimizer->filters.size(); i++) {
		auto &filter = join_optimizer->filters[i];
		// first extract the relation set for the entire filter
		unordered_set<idx_t> relations;
		ExtractRelationBindings(*filter, relations);
		auto &set = join_optimizer->set_manager.GetJoinRelation(relations);
		auto info = make_uniq<FilterInfo>(set, i);

		auto filter_info = info.get();
		join_optimizer->filter_infos.push_back(std::move(info));

		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();
			// extract the relation_ids that are required for the left and right side of the comparison
			unordered_set<idx_t> left_relations, right_relations;
			ExtractRelationBindings(*comparison.left, left_relations);
			ExtractRelationBindings(*comparison.right, right_relations);
			// Get Column Bindings to know exactly what columns between relations are being joined with each other
			// In the function GetColumnBindings we can add debug info telling us exactly what filters we have gathered.
			GetColumnBinding(*comparison.left, filter_info->left_binding);
			GetColumnBinding(*comparison.right, filter_info->right_binding);
			filter_info->left_join_column = GetFilterString(left_relations, comparison.left->ToString());
			filter_info->right_join_column = GetFilterString(right_relations, comparison.right->ToString());
			if (!left_relations.empty() && !right_relations.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = &join_optimizer->set_manager.GetJoinRelation(left_relations);
				filter_info->right_set = &join_optimizer->set_manager.GetJoinRelation(right_relations);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_relations, right_relations)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						join_optimizer->query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set,
						                       filter_info);
						join_optimizer->query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set,
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
}


} // namespace duckdb
