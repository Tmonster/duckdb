//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class JoinOrderOptimizer;

class EstimatedProperties {
public:
	idx_t cost;
	double cardinality;

	unique_ptr<EstimatedProperties> Copy();
private:
};

class JoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet *set;
	NeighborInfo *info;
	double cardinality;
	double cost;
	bool has_filter;
	JoinNode *left;
	JoinNode *right;

	ColumnBinding left_binding;
	ColumnBinding right_binding;
	unique_ptr<EstimatedProperties> estimated_props;

	//! Create a leaf node in the join tree
	//! set cost to 0 because leaf nodes/base table already exist
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet *set, double cardinality)
	    : set(set), info(nullptr), cardinality(cardinality), cost(0), has_filter(false), left(nullptr), right(nullptr),
	      left_binding(), right_binding(){
		estimated_props = make_unique<EstimatedProperties>();
	}
	//! Create an intermediate node in the join tree
	JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, double cardinality, double cost)
	    : set(set), info(info), cardinality(cardinality), cost(cost), has_filter(false), left(left), right(right),
	      left_binding(), right_binding() {
		estimated_props = make_unique<EstimatedProperties>();
	}

	static idx_t hash_table_col(idx_t table, idx_t col);

	void check_all_table_keys_forwarded();

public:
	void UpdateCost();

	//! debugging functions
	static bool DesiredRelationSet(JoinRelationSet *relation_set, unordered_set<idx_t> o_set);
	static bool DesiredJoin(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left,
	                         unordered_set<idx_t> desired_right);
};

} // namespace duckdb
