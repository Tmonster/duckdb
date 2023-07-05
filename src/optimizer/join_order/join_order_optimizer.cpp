#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>
#include <cmath>

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(const unordered_set<T> &a, const unordered_set<T> &b) {
	return std::all_of(a.begin(), a.end(), [&b](typename std::unordered_set<T>::const_reference entry) {
		return b.find(entry) == b.end();
	});
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	D_ASSERT(filters.empty() && relations.empty()); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();

	// now we begin optimizing the current plan
	// Skip all operators until we find the first projection, we do this because the HAVING clause inserts a Filter
	// AFTER the group by and this filter cannot be reordered Then we extract a list of all relations that have to be
	// joined together and a list of all conditions/join filters that are applied to them
	// Here we need a parent op for some reason. Makes no sense to me.
	if (!relation_extractor.ExtractJoinRelations(*op)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	// TODO: This should return the filters to make the code more declarative
	relation_extractor.ExtractFilters();

	// create edges between relations from the join comparisons
	// TODO: this should return a query graph to make the code more declarative
	relation_extractor.CreateQueryGraph();

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
	auto final_plan = join_enumerator.SolveJoinOrder(context.config.force_no_cross_product);

	// now perform the actual reordering
	return plan_rewriter.RewritePlan(std::move(plan), *final_plan);
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
			binding = ColumnBinding(table_index, binding.column_index);
			return &proj;
		}
		D_ASSERT(false);
		// no expressions in the projection come from a bound_column_ref
		return &proj;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		// In set operations, both children have the same number of result columns
		// we can grab the binding from either the first child or the second child.
		// lets just go into the left child. Sometimes the child doesn't have a table index (can be another set
		// operations) so we recurse until we find a operation that has a table index
		auto bindings = op.GetColumnBindings();
		auto set_table_index = op.GetTableIndex();
		D_ASSERT(table_index != DConstants::INVALID_INDEX);
		auto left_child = op.children.at(0).get();
		auto left_child_table_index = left_child->GetTableIndex();
		while (left_child_table_index.size() == 0) {
			left_child = left_child->children.at(0).get();
			left_child_table_index = left_child->GetTableIndex();
		}
		// what if it is a join?
		if (left_child_table_index.size() > 1) {
			throw InternalException("Deal with case where union operator is on top of set operator.");
		}
		binding = ColumnBinding(left_child_table_index.at(0), binding.column_index);
		get = GetDataRetOp(*left_child, binding);
		if (get) {
			return get;
		}
		D_ASSERT(false);
		break;
	}
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
				binding = expression_binding_translation.new_binding;
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
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		if (binding.column_index >= distinct.distinct_targets.size() || distinct.distinct_targets.empty()) {
			break;
		}

		auto new_expression = distinct.distinct_targets.at(binding.column_index).get();

		auto expression_binding_translation = RecursiveEnumerateProjectionExpressions(new_expression, binding);
		if (expression_binding_translation.found_expression && !expression_binding_translation.expression_is_constant) {
			// we have an expression at the binding. If the
			auto ret = GetDataRetOp(*op.children.at(0), expression_binding_translation.new_binding);
			if (ret) {
				binding = expression_binding_translation.new_binding;
				return ret;
			}
		}
		if (expression_binding_translation.expression_is_constant) {
			return &distinct;
		}
		D_ASSERT(false);
		return &distinct;
	}
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

} // namespace duckdb
