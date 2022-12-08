//
// Created by Tom Ebergen on 06/12/2022.
//

#include "duckdb/optimizer/operator_pool.hpp"

namespace duckdb {

void OperatorPool::CheckNotOptimized(LogicalOperator *op) {
	D_ASSERT(!InPool(op));
#ifdef DEBUG
	seen_operators.insert((idx_t)op);
#endif
}

bool OperatorPool::InPool(LogicalOperator *op) {
	auto it = seen_operators.find((idx_t)op);
	return it != seen_operators.end();
}

void OperatorPool::EmptyOperatorPool() {
	seen_operators.clear();
}

} // namespace duckdb
