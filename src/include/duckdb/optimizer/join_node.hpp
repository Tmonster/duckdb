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

#include <map>

#include <functional>

namespace duckdb {

class JoinOrderOptimizer;

struct TableFilterStats {
	idx_t cardinality_with_equality_filter;
	bool has_equality_filter;
};

class JoinStats {
public:
	idx_t base_table_left;
	idx_t base_table_right;
	idx_t base_column_left;
	idx_t base_column_right;

	idx_t cost;
	double cardinality;

	JoinStats() : base_table_left(0), base_table_right(0), base_column_left(0), base_column_right(0),
	      cost(0), cardinality(0) {
	}

	std::unique_ptr<JoinStats> Copy(std::unique_ptr<JoinStats> join_stats) {
		base_table_left = join_stats->base_table_left;
		base_table_right = join_stats->base_table_right;
		base_column_left = join_stats->base_column_left;
		base_column_right = join_stats->base_column_right;

	  	cost = join_stats->cost;
		cardinality = join_stats->cardinality;
		return join_stats;
	}

	JoinStats(const JoinStats &) = delete;
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

	unique_ptr<JoinStats> join_stats;

	//! have the multiplicity and selectivity stats been initialized?
	bool init_stats;
	bool created_as_intermediate;

	//! Create a leaf node in the join tree
	//! set cost to 0 because leaf nodes/base table already exist
	//! cost will be the cost to *produce* an intermediate table
	JoinNode(JoinRelationSet *set, double cardinality)
	    : set(set), info(nullptr), cardinality(cardinality), cost(0), has_filter(false), left(nullptr), right(nullptr), init_stats(false),
	      created_as_intermediate(false) {
		join_stats = make_unique<JoinStats>();
	}
	//! Create an intermediate node in the join tree
	JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, double cardinality, double cost)
	    : set(set), info(info), cardinality(cardinality), cost(cost), has_filter(false), left(left), right(right), init_stats(true),
	      created_as_intermediate(true) {
		join_stats = make_unique<JoinStats>();
	}


	static idx_t hash_table_col(idx_t table, idx_t col);
	static idx_t readable_hash(idx_t table, idx_t col);
	static idx_t GetColumnFromReadableHash(idx_t hash);
	static idx_t GetTableFromReadableHash(idx_t hash);

	void check_all_table_keys_forwarded();
	unique_ptr<TableFilterStats> InspectTableFilters(TableFilterSet *filters, TableCatalogEntry *catalog_table, JoinOrderOptimizer *optimizer);

public:

	idx_t GetTDom(idx_t table, idx_t column, JoinOrderOptimizer *optimizer);
	void UpdateCardinalityEstimate(JoinOrderOptimizer *optimizer);

	void UpdateCost();

	TableCatalogEntry* GetCatalogTableEntry(LogicalOperator *op);
	static bool key_exists(idx_t key, unordered_map<idx_t, double> stat_column);
	void InitColumnStats(JoinOrderOptimizer *optimizer);
	void InitTDoms(JoinOrderOptimizer *optimizer);

	//! debugging functions
	static bool desired_relation_set(JoinRelationSet *relation_set, unordered_set<idx_t> o_set);
	static bool desired_join(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left,
	                         unordered_set<idx_t> desired_right);
};

} // namespace duckdb
