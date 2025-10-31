#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                              unordered_set<idx_t> &left_bindings,
                                                              unordered_set<idx_t> &right_bindings) {
	auto &join = op->Cast<LogicalJoin>();
	D_ASSERT(join.join_type == JoinType::INNER);
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		// try to push the current filters into the children.
		unordered_set<idx_t> remove_filters;
		for (idx_t i = 0; i < join.children.size(); i++) {
			FilterPushdown new_pushdown(optimizer, convert_mark_joins);
			unordered_set<idx_t> child_bindings;
			LogicalJoin::GetTableReferences(*op->children[i], child_bindings);
			// only add filters to the children if we know the bindings are in the join child
			for (idx_t j = 0; j < filters.size(); j++) {
				auto &filter = filters[j];
				bool contains_all_bindings =
				    std::all_of(filter->bindings.begin(), filter->bindings.end(),
				                [&](const idx_t &binding) { return child_bindings.count(binding); });
				if (contains_all_bindings) {
					new_pushdown.AddFilter(filter->filter->Copy());
					remove_filters.insert(j);
				}
			}
			new_pushdown.GenerateFilters();
			if (!new_pushdown.filters.empty()) {
				join.children[i] = new_pushdown.Rewrite(std::move(join.children[i]));
			}
		}
		for (auto &pushed_filter : remove_filters) {
			if (pushed_filter < filters.size()) {
				filters.erase(filters.begin() + static_cast<int64_t>(pushed_filter));
			}
		}
		// TODO: prevent recursive calls since the Rewrites above will already
		// pushdown extra filters
		return FinishPushdown(std::move(op));
	}
	// inner join: gather all the conditions of the inner join and add to the filter list
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &any_join = join.Cast<LogicalAnyJoin>();
		// any join: only one filter to add
		if (AddFilter(std::move(any_join.condition)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	} else {
		// comparison join
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		// turn the conditions into filters
		for (auto &i : comp_join.conditions) {
			auto condition = JoinCondition::CreateExpression(std::move(i));
			if (AddFilter(std::move(condition)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
		}
	}
	GenerateFilters();

	// turn the inner join into a cross product
	auto cross_product = make_uniq<LogicalCrossProduct>(std::move(op->children[0]), std::move(op->children[1]));

	// preserve the estimated cardinality of the operator
	if (op->has_estimated_cardinality) {
		cross_product->SetEstimatedCardinality(op->estimated_cardinality);
	}

	// then push down cross product
	return PushdownCrossProduct(std::move(cross_product));
}

} // namespace duckdb
