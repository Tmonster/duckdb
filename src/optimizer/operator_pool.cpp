//
// Created by Tom Ebergen on 06/12/2022.
//

#include "duckdb/optimizer/operator_pool.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> OperatorPool::AddOperator(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!InPool(op));
	seen_operators.insert(op);
	return move(op);
}

bool OperatorPool::InPool(unique_ptr<LogicalOperator> op) {
	auto it = seen_operators.find(op);
	return it != seen_operators.end();
}

}