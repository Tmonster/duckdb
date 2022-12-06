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
		seen_operators = unordered_set<unique_ptr<LogicalOperator>>();
	}

	unique_ptr<LogicalOperator> AddOperator(unique_ptr<LogicalOperator> op);
	bool InPool(unique_ptr<LogicalOperator> op);

private:
	unordered_set<unique_ptr<LogicalOperator>> seen_operators;
};
}