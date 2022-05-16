#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = JoinTypeToString(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr = make_unique<BoundComparisonExpression>(condition.comparison, condition.left->Copy(),
		                                                   condition.right->Copy());
		result += expr->ToString();
	}
	result += "\nEC = " + std::to_string(estimated_cardinality) + "\n";
//	result += "\nleft sel = " + std::to_string(left_col_sel) + "\n";
//	result += "\nleft mul = " + std::to_string(left_col_mult) + "\n";
//	result += "\nright sel = " + std::to_string(right_col_sel) + "\n";
//	result += "\nright mul = " + std::to_string(right_col_mult) + "\n";

	return result;
}

} // namespace duckdb
