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
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <map>

#include <functional>



namespace duckdb {

enum class mult_sel : uint8_t {
	left_table_mults,
	right_table_mults,
	left_table_sels,
	right_table_sels
};

class JoinOrderOptimizer;

class JoinNode {
public:
	//! Represents a node in the join plan
	JoinRelationSet *set;
	NeighborInfo *info;
	idx_t cardinality;
	idx_t cost;
	JoinNode *left;
	JoinNode *right;


	struct JoinStats {
		idx_t base_table_left;
		idx_t base_table_right;
		idx_t base_column_left;
		idx_t base_column_right;

		double cardinality_ratio;
		double left_col_sel;
		double left_col_mult;
		double right_col_sel;
		double right_col_mult;

		unordered_map<idx_t, unordered_set<idx_t>> table_cols;

		unordered_map<idx_t, double> table_col_mults;
		unordered_map<idx_t, double> table_col_sels;
	};

	struct JoinStats join_stats;

	//! have the multiplicity and selectivity stats been initialized?
	bool init_stats;
	bool created_as_intermediate;

	// first idx is a 32 bits for table id
	//                32 bits for column id
	// you need to use some bit manipulation to get he numbers

	//! Create a leaf node in the join tree
	//! set cost
	//! to 0 because leaf nodes/base table already exist
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet *set, idx_t cardinality)
		: set(set), info(nullptr), cardinality(cardinality), cost(0), left(nullptr), right(nullptr),
		  init_stats(false), created_as_intermediate(false) {
		join_stats.base_table_left = 0;
		join_stats.base_table_right = 0;
		join_stats.base_column_left = 0;
		join_stats.base_column_right = 0;
	}
	//! Create an intermediate node in the join tree
	JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, idx_t cardinality,
			 idx_t cost)
		: set(set), info(info), cardinality(cardinality), cost(cost), left(left), right(right), init_stats(false),
		  created_as_intermediate(true) {
		init_stats = true;
		join_stats.table_cols = unordered_map<idx_t, unordered_set<idx_t>>();
		join_stats.table_col_mults = unordered_map<idx_t, double>();
		join_stats.table_col_sels = unordered_map<idx_t, double>();

		//! need to refactor these stats into another columns
		join_stats.base_table_left = 1;
		join_stats.base_table_right = 1;
		join_stats.base_column_left = 1;
		join_stats.base_column_right = 1;
		join_stats.cardinality_ratio = 1;

		join_stats.left_col_sel = 1;
		join_stats.left_col_mult = 1;
		join_stats.right_col_sel = 1;
		join_stats.right_col_mult = 1;
	}

	static idx_t hash_table_col(idx_t table, idx_t col);

	void check_all_table_keys_forwarded();

public:
	void update_cardinality_estimate(bool same_base_table);
	void update_cost();

	void update_cardinality_ratio(bool same_base_table);

	void update_stats_from_left_table(idx_t left_pair_key, idx_t right_pair_key);
	void update_stats_from_right_table(idx_t left_pair_key, idx_t right_pair_key);

	static bool key_exists(idx_t key, unordered_map<idx_t, double> stat_column);
	void InitColumnStats(vector<FilterInfo *> filters, JoinOrderOptimizer *optimizer);
	//! debugging functions
	static bool desired_relation_set(JoinRelationSet *relation_set, unordered_set<idx_t> o_set);
	static bool desired_join(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left, unordered_set<idx_t> desired_right);
	static void printWholeNode(JoinNode *node);
	static void PrintNodeSelMulStats(JoinNode *node);
};

} // namespace duckdb
