//
// Created by Tom Ebergen on 5/16/22.
//
#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/optimizer/join_node.hpp"

namespace duckdb {

void JoinNode::UpdateCost() {
	cost = cardinality + left->cost + right->cost;

	estimated_props->cost = cost;
}


//! ******************************************************
//! *          START OF DEBUGGING FUNCTIONS              *
//! ******************************************************
bool JoinNode::DesiredRelationSet(JoinRelationSet *relation_set, unordered_set<idx_t> o_set) {
	for (idx_t it = 0; it < relation_set->count; it++) {
		if (o_set.find(relation_set->relations[it]) == o_set.end()) {
			return false;
		}
	}
	return relation_set->count == o_set.size();
}

bool JoinNode::DesiredJoin(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left,
                            unordered_set<idx_t> desired_right) {
	bool left_is_left = DesiredRelationSet(left, desired_left) && DesiredRelationSet(right, desired_right);
	bool right_is_left = DesiredRelationSet(right, desired_left) && DesiredRelationSet(left, desired_right);
	return left_is_left || right_is_left;
}
} // namespace duckdb
