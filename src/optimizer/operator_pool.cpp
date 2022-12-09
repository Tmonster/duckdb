#include "duckdb/optimizer/operator_pool.hpp"

namespace duckdb {

void OperatorPool::AssertNotInPool(LogicalOperator *op) {
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
