//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/operator_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class OperatorPool {
public:
	OperatorPool() {
		seen_operators = unordered_set<idx_t>();
	}

	void AddOperator(LogicalOperator *op);
	bool InPool(LogicalOperator *op);
	void EmptyOperatorPool();

private:
	unordered_set<idx_t> seen_operators;
};
} // namespace duckdb
