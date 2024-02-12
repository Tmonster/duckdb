//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/operator/logical_unconditional_join.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalUnconditionalJoin {
	LogicalCrossProduct() : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {};

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CROSS_PRODUCT;

	//! The columns of the LHS that are output by the join
	vector<idx_t> left_projection_map;
	//! The columns of the RHS that are output by the join
	vector<idx_t> right_projection_map;

public:
	LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	vector<ColumnBinding> GetColumnBindings() override;
public:
	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
