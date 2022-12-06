//
// Created by Tom Ebergen on 06/12/2022.
//

#include "duckdb/optimizer/operator_pool.hpp"

namespace duckdb {

void OperatorPool::AddOperator(LogicalOperator *op) {
	D_ASSERT(!InPool(op));
	seen_operators.insert((idx_t)op);
}

bool OperatorPool::InPool(LogicalOperator *op) {
	auto it = seen_operators.find((idx_t)op);
	return it != seen_operators.end();
}

}