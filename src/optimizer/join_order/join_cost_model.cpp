
#include "duckdb/optimizer/join_order/join_cost_model.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {


vector<NodeOp> JoinCostModel::Init(vector<unique_ptr<SingleJoinRelation>> &relations_, JoinRelationSetManager &set_manager_){
	auto node_ops = cardinality_estimator.InitCardinalityEstimatorProps();
	relations = &relations_;
	set_manager = &set_manager_;
	return node_ops;
}


// The cost of every join is the cost of the estimated cardinality of the join
// First we calcualte the union of the set, then we estimate the cardinality.
// TODO: need to manage cross products.
double JoinCostModel::compute_cost(JoinNode &left, JoinNode &right) {
	auto &set = set_manager->Union(left.set, right.set);

	auto cardinality_of_join = cardinality_estimator.GetCardinality(set);

	auto total_cost = left.GetCost() + right.GetCost() + cardinality_of_join;
	return total_cost;
}

void JoinCostModel::VerifySymmetry(JoinNode &result, JoinNode &entry) {
	if (result.GetCardinality<double>() != entry.GetCardinality<double>()) {
		// Currently it's possible that some entries are cartesian joins.
		// When this is the case, you don't always have symmetry, but
		// if the cost of the result is less, then just assure the cardinality
		// is also less, then you have the same effect of symmetry.
		D_ASSERT(ceil(result.GetCardinality<double>()) <= ceil(entry.GetCardinality<double>()) ||
		         floor(result.GetCardinality<double>()) <= floor(entry.GetCardinality<double>()));
	}
}

} // namespace duckdb
