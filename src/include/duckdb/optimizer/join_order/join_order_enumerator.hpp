//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_enumerator.hpp
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

class JoinOrderEnumerator {
public:
	explicit JoinOrderEnumerator(ClientContext &context, JoinOrderOptimizer *optimizer)
	    : context(context), join_optimizer(optimizer), full_plan_found(false), must_update_full_plan(false) {
	}

	//! The optimal join plan found for the specific JoinRelationSet*
	unordered_map<JoinRelationSet *, unique_ptr<JoinNode>> plans;
	//! The set of edges used in the join optimizer
	QueryGraph query_graph;

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	JoinOrderOptimizer *join_optimizer;

	//! The total amount of join pairs that have been considered
	idx_t pairs = 0;

	bool full_plan_found;
	bool must_update_full_plan;
	unordered_set<std::string> join_nodes_in_full_plan;

public:
	//! Perform the join order solving
	void SolveJoinOrder();
	//! Generate cross product edges inside the side
	void GenerateCrossProducts();

private:
	//! Emit a pair as a potential join candidate. Returns the best plan found for the (left, right) connection (either
	//! the newly created plan, or an existing plan)
	JoinNode &EmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);
	//! Tries to emit a potential join candidate pair. Returns false if too many pairs have already been emitted,
	//! cancelling the dynamic programming step.
	bool TryEmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);
	bool EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right, unordered_set<idx_t> exclusion_set);
	//! Emit a relation set node
	bool EmitCSG(JoinRelationSet &node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	bool EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set);
	//! Solve the join order exactly using dynamic programming. Returns true if it was completed successfully (i.e. did
	//! not time-out)
	bool SolveJoinOrderExactly();
	//! Solve the join order approximately using a greedy algorithm
	void SolveJoinOrderApproximately();

	unique_ptr<JoinNode> CreateJoinTree(JoinRelationSet &set,
	                                    const vector<reference<NeighborInfo>> &possible_connections, JoinNode &left,
	                                    JoinNode &right);

	void UpdateDPTree(JoinNode &new_plan);

	void UpdateJoinNodesInFullPlan(JoinNode &node);
	bool NodeInFullPlan(JoinNode &node);
};

} // namespace duckdb
