//
// Created by Tom Ebergen on 5/16/22.
//
#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/optimizer/join_node.hpp"

namespace duckdb {

unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
	auto result = make_unique<EstimatedProperties>();
	result->cost = cost;
	result->cardinality = cardinality;
	return result;
}

void JoinNode::UpdateCost() {
	cost = cardinality + left->cost + right->cost;
	estimated_props->cost = cost;
}

string JoinNode::ToString() {
	if (!set) {
		return "";
	}
	string result = "-------------------------------\n";
	result += set->ToString() + "\n";
	result += "card = " + to_string(estimated_props->cardinality) + "\n";
	bool is_cartesian = false;
	if (left && right) {
		is_cartesian = (cardinality == left->estimated_props->cardinality * right->estimated_props->cardinality);
	}
	result += "cartesian = " + to_string(is_cartesian) + "\n";
	result += "cost = " + to_string(estimated_props->cost) + "\n";
	result += "left = \n";
	if (left) {
		result += left->ToString();
	}
	result += "right = \n";
	if (right) {
		result += right->ToString();
	}
	return result;
}
} // namespace duckdb
