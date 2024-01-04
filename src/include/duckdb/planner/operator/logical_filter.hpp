//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

static bool CanFiltersPropogateRightSide(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::SEMI) {
		return false;
	}
	auto left_bindings = op.children[0]->GetColumnBindings();
	auto right_bindings = op.children[1]->GetColumnBindings();
	D_ASSERT(left_bindings.size() == right_bindings.size());
	// make sure we are comparing every column
	if (join.conditions.size() != left_bindings.size()) {
		return false;
	}
	auto &conditions = join.conditions;
	for (idx_t i = 0; i < conditions.size(); i++) {
		auto &cond = conditions[i];
		auto &left = cond.left;
		auto &right = cond.right;
		if (cond.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			if (left->type == ExpressionType::BOUND_COLUMN_REF && right->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &left_expr = left->Cast<BoundColumnRefExpression>();
				auto &right_expr = right->Cast<BoundColumnRefExpression>();
				auto left_match = left_expr.binding == left_bindings[i];
				auto right_match = right_expr.binding == right_bindings[i];
				if (!(left_match && right_match)) {
					return false;
				}
			}
		}
	}
	return true;
}

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_FILTER;

public:
	explicit LogicalFilter(unique_ptr<Expression> expression);
	LogicalFilter();

	vector<idx_t> projection_map;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	bool SplitPredicates() {
		return SplitPredicates(expressions);
	}
	//! Splits up the predicates of the LogicalFilter into a set of predicates
	//! separated by AND Returns whether or not any splits were made
	static bool SplitPredicates(vector<unique_ptr<Expression>> &expressions);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
