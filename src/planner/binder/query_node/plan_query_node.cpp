#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = mod->Cast<BoundDistinctModifier>();
			auto distinct = make_uniq<LogicalDistinct>(std::move(bound.target_distincts), bound.distinct_type);
			distinct->AddChild(std::move(root));
			auto proj_index = GenerateTableIndex();
			vector<unique_ptr<Expression>> projection_list;
			auto distinct_types = distinct->types;
			auto distinct_bindings = distinct->GetColumnBindings();
			D_ASSERT(distinct->types.size() == distinct->GetColumnBindings().size());
			for (idx_t i = 0; i < distinct->types.size(); i++) {
				projection_list.push_back(make_uniq<BoundColumnRefExpression>(distinct_types.at(i), distinct_bindings.at(i)));
			}
			auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_list));
			proj->children.push_back(std::move(distinct));
			root = std::move(proj);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &bound = mod->Cast<BoundOrderModifier>();
			if (root->type == LogicalOperatorType::LOGICAL_DISTINCT) {
				auto &distinct = root->Cast<LogicalDistinct>();
				if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
					auto order_by = make_uniq<BoundOrderModifier>();
					for (auto &order_node : bound.orders) {
						order_by->orders.push_back(order_node.Copy());
					}
					distinct.order_by = std::move(order_by);
				}
			}
			auto order = make_uniq<LogicalOrder>(std::move(bound.orders));
			order->AddChild(std::move(root));
			root = std::move(order);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &bound = mod->Cast<BoundLimitModifier>();
			auto limit = make_uniq<LogicalLimit>(std::move(bound.limit_val), std::move(bound.offset_val));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		default:
			throw BinderException("Unimplemented modifier type!");
		}
	}
	return root;
}

} // namespace duckdb
