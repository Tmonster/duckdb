//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/operator_pool.hpp
//
//
//===----------------------------------------------------------------------===//

namespace duckdb {

struct LogicalOperatorIdentifier {
	LogicalOperatorType type;
	string name;

};

class OperatorPool {
public:
	void AddOperator(unique_ptr<LogicalOperator> op);
	bool InPool(unique_ptr<LogicalOperator> op);

private:
	unordered_set<LogicalOperatorIdentifier> seen_operators;
};
}