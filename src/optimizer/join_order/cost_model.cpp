#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

static double GetJoinFilterMultipler(FilterInfo &filter) {
	double multiplier = 1;
	ExpressionIterator::EnumerateExpression(filter.filter, [&](Expression &child) {
	if (child.expression_class == ExpressionClass::BOUND_COMPARISON) {
		switch (child.type) {
		case ExpressionType::COMPARE_EQUAL:
			multiplier = 1;
		case ExpressionType::COMPARE_NOTEQUAL:
			multiplier = 2;
		case ExpressionType::
		}
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		bindings_to_keep.push_back(col_ref.binding);
	}
});
	switch (filter.filter->type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::
	}
}

double CostModel::ComputeCost(DPJoinNode &left, DPJoinNode &right, NeighborInfo &filter) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);

	double multiplier = 1;
	for (auto &f : filter.filters) {
		multiplier = GetJoinFilterMultipler(*f);
	}

	auto join_cost = join_card * multiplier;
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb
