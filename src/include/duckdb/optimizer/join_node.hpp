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

#include <map>

#include <functional>

namespace duckdb {

class JoinOrderOptimizer;


class JoinStats {
public:
	idx_t base_table_left;
	idx_t base_table_right;
	idx_t base_column_left;
	idx_t base_column_right;

	double cardinality_ratio;
	double left_col_sel;
	double left_col_mult;
	double right_col_sel;
	double right_col_mult;

	idx_t cost;
	idx_t cardinality;

	unordered_map<idx_t, unordered_set<idx_t>> table_cols;
	unordered_map<idx_t, double> table_col_unique_vals;


	JoinStats() : base_table_left(0), base_table_right(0), base_column_left(0), base_column_right(0),
	      cardinality_ratio(1), left_col_sel(1), left_col_mult(1), right_col_sel(1), right_col_mult(1), cost(0), cardinality(0) {
		table_cols = unordered_map<idx_t, unordered_set<idx_t>>();
		table_col_unique_vals = unordered_map<idx_t, double>();
	}

	std::unique_ptr<JoinStats> Copy(std::unique_ptr<JoinStats> join_stats) {
	    base_table_left = join_stats->base_table_left;
		base_table_right = join_stats->base_table_right;
		base_column_left = join_stats->base_column_left;
	    base_column_right = join_stats->base_column_right;
	    cardinality_ratio = join_stats->cardinality_ratio;
	    left_col_sel = join_stats->left_col_sel;
	    left_col_mult = join_stats->left_col_mult;
	    right_col_sel = join_stats->right_col_sel;
	  	right_col_mult = join_stats->right_col_mult;

	  	cost = join_stats->cost;
		cardinality = join_stats->cardinality;

		// these should be updated using JoinNode functions
		table_cols = unordered_map<idx_t, unordered_set<idx_t>>();
		unordered_map<idx_t, double>::iterator it;
		for (it = join_stats->table_col_unique_vals.begin(); it != join_stats->table_col_unique_vals.end(); it++) {
			table_col_unique_vals[it->first] = it->second;
		}
		return join_stats;
	}

	JoinStats(const JoinStats &) = delete;

};

class JoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet *set;
	NeighborInfo *info;
	idx_t cardinality;
	idx_t cost;
	bool has_filter;
	JoinNode *left;
	JoinNode *right;

	std::unique_ptr<JoinStats> join_stats;

	//! have the multiplicity and selectivity stats been initialized?
	bool init_stats;
	bool created_as_intermediate;

	//! Create a leaf node in the join tree
	//! set cost to 0 because leaf nodes/base table already exist
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet *set, idx_t cardinality)
	    : set(set), info(nullptr), cardinality(cardinality), cost(0), has_filter(false), left(nullptr), right(nullptr), init_stats(false),
	      created_as_intermediate(false) {
		join_stats = make_unique<JoinStats>();
	}
	//! Create an intermediate node in the join tree
	JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, idx_t cardinality, idx_t cost)
	    : set(set), info(info), cardinality(cardinality), cost(cost), has_filter(false), left(left), right(right), init_stats(true),
	      created_as_intermediate(true) {
		join_stats = make_unique<JoinStats>();
	}


	static idx_t hash_table_col(idx_t table, idx_t col);

	void check_all_table_keys_forwarded();

public:
	void update_cardinality_estimate(bool same_base_table);
	void update_cost();

	void update_stats_from_joined_tables(idx_t left_pair_key, idx_t right_pair_key);

	static bool key_exists(idx_t key, unordered_map<idx_t, double> stat_column);
	void InitColumnStats(vector<FilterInfo *> filters, JoinOrderOptimizer *optimizer);
	double GetTableColMult(idx_t table, idx_t col);
	//! debugging functions
	static bool desired_relation_set(JoinRelationSet *relation_set, unordered_set<idx_t> o_set);
	static bool desired_join(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left,
	                         unordered_set<idx_t> desired_right);
	static void printWholeNode(JoinNode *node);
	static void PrintNodeUniqueValueStats(JoinNode *node);
};

} // namespace duckdb
