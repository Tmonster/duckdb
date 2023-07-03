//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_cost_model.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

class JoinOrderOptimizer;

class JoinCostModel {
public:
	explicit JoinCostModel(ClientContext &context) : cardinality_estimator(context) {}

private:
	//! Cardinality Estimator
	CardinalityEstimator cardinality_estimator;
	vector<unique_ptr<SingleJoinRelation>> *relations;
	JoinRelationSetManager *set_manager;


public:
	vector<NodeOp> Init(vector<unique_ptr<SingleJoinRelation>> &relations, JoinRelationSetManager &set_manager_);
	double compute_cost(JoinNode &left, JoinNode &right);

	static void VerifySymmetry(JoinNode &result, JoinNode &entry);

private:

};

} // namespace duckdb
