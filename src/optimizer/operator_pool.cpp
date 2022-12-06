//
// Created by Tom Ebergen on 06/12/2022.
//

namespace duckdb {

void OperatorPool::AddOperator(unique_ptr<LogicalOperator> op) {

}

bool OperatorPool::InPool(unique_ptr<LogicalOperator> op) {
	return false;
}

}