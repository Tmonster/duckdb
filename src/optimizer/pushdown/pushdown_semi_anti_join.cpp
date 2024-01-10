#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceBindings(vector<ColumnBinding> &bindings, Filter &filter, Expression &expr,
                            vector<ColumnBinding> &replacement_bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		idx_t binding_index = 0;
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == colref.binding) {
				binding_index = i;
				break;
			}
		}
		colref.binding = replacement_bindings[binding_index];
		filter.bindings.insert(colref.binding.table_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceBindings(bindings, filter, child, replacement_bindings); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSemiAntiJoin(unique_ptr<LogicalOperator> op) {
	auto &join = op->Cast<LogicalJoin>();
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return FinishPushdown(std::move(op));
	}

	if (CanFiltersPropogateRightSide(*op)) {
		auto left_bindings = op->children[0]->GetColumnBindings();
		auto right_bindings = op->children[1]->GetColumnBindings();
		FilterPushdown right_pushdown(optimizer);
		FilterPushdown left_pushdown(optimizer);
		for (idx_t i = 0; i < filters.size(); i++) {
			// first create a copy of the filter
			auto right_filter = make_uniq<Filter>();
			right_filter->filter = filters[i]->filter->Copy();
			auto left_filter = make_uniq<Filter>();
			left_filter->filter = filters[i]->filter->Copy();

			ReplaceBindings(left_bindings, *right_filter, *right_filter->filter, right_bindings);
			ReplaceBindings(right_bindings, *left_filter, *left_filter->filter, left_bindings);
			right_filter->ExtractBindings();
			left_filter->ExtractBindings();

			// move the filters into the child pushdown nodes
			right_pushdown.filters.push_back(std::move(right_filter));
			left_pushdown.filters.push_back(std::move(left_filter));
		}
		// If filter pulled up from right, need to replace bindings on left.
		op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
		op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));
	} else {
		// push all current filters down the left side
		op->children[0] = Rewrite(std::move(op->children[0]));
	}

	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && right_empty) {
		// both empty: return empty result
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}
	// TODO: if semi/anti join is created from a intersect/except statement, then we can
	//  push filters down into both children.
	// filter pushdown happens before join order optimization, so right_anti and right_semi are not possible yet here
	if (left_empty) {
		// left child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	} else if (right_empty) {
		// right child is empty result
		switch (join.join_type) {
		case JoinType::ANTI:
			// just return the left child.
			return std::move(op->children[0]);
		case JoinType::SEMI:
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			break;
		}
	}
	return op;
}

} // namespace duckdb
