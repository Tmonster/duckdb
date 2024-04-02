#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	ColumnBindingReplacer bindings_replacer;
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = mod->Cast<BoundDistinctModifier>();
			auto distinct = make_uniq<LogicalDistinct>(std::move(bound.target_distincts), bound.distinct_type);
			distinct->AddChild(std::move(root));
			root = std::move(distinct);
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
//	auto proj_index = GenerateTableIndex();
//	vector<unique_ptr<Expression>> projection_list;
//	auto distinct_bindings = root->GetColumnBindings();
//	for (auto &binding : root->GetColumnBindings()) {
//		auto to_replace = ReplacementBinding(binding, ColumnBinding(proj_index, binding.column_index));
//		bindings_replacer.replacement_bindings.push_back(to_replace);
//	}
//	vector<unique_ptr<Expression>> expressions;
//	switch (root->type) {
//	case LogicalOperatorType::LOGICAL_LIMIT:
//		break;
//	case LogicalOperatorType::LOGICAL_ORDER_BY: {
//		auto &order = root->Cast<LogicalOrder>();
//		for (auto &target : order.)
//		break;
//	}
//	case LogicalOperatorType::LOGICAL_DISTINCT:
//		auto &distinct = root->Cast<LogicalDistinct>();
//		for (auto &target : distinct.distinct_targets) {
//			expressions.push_back(target->Copy());
//		}
//	}
//	for (auto &expression : expressions) {
//		bindings_replacer.VisitExpression(&expression);
//		projection_list.push_back(std::move(expression));
//	}
//	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_list));
//	proj->children.push_back(std::move(root));
	return root;
}

} // namespace duckdb
