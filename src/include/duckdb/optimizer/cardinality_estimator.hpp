//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/join_node.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

static constexpr double DEFAULT_SELECTIVITY = 0.2;

class CardinalityEstimator {
public:
	explicit CardinalityEstimator(ClientContext &context) : context(context) {
	}

private:
	ClientContext &context;

	//! A mapping of base table index -> all columns used to determine the join order
	unordered_map<idx_t, unordered_set<idx_t>> relation_to_columns;
	//! A mapping of (relation, bound_column) -> (actual table, actual column)
	column_binding_map_t<ColumnBinding> relation_column_to_original_column;
	vector<column_binding_set_t> equivalent_relations;
	vector<idx_t> equivalent_relations_tdom_no_hll;
	vector<idx_t> equivalent_relations_tdom_hll;
	unordered_map<idx_t, std::string> relation_to_table_name;

public:
	void AssertEquivalentRelationSize();
	void AddRelationToColumnMapping(ColumnBinding key, ColumnBinding value);
	void AddColumnToRelationMap(idx_t table_index, idx_t column_index);
	void CopyRelationMap(column_binding_map_t<ColumnBinding> &child_binding_map);

	void InitTotalDomains();
	void UpdateTotalDomains(JoinNode *node, LogicalOperator *op, vector<unique_ptr<FilterInfo>> *filter_infos);
	void InitEquivalentRelations(vector<unique_ptr<FilterInfo>> *filter_infos);

	void EstimateCardinality(JoinNode *node);
	void EstimateBaseTableCardinality(JoinNode *node, LogicalOperator *op);

private:
	bool SingleColumnFilter(FilterInfo *filter_info);
	//! Filter & bindings -> list of indexes into the equivalent_relations array.
	// The column binding set at each index is an equivalence set.
	vector<idx_t> DetermineMatchingEquivalentSets(FilterInfo *filter_info);

	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets);

	idx_t GetTDom(ColumnBinding binding);
	TableFilterSet *GetTableFilters(LogicalOperator *op);

	idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter *fil,
	                            TableCatalogEntry *catalog_table);
	idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter *fil,
	                           TableCatalogEntry *catalog_table);
	idx_t InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters);
};

} // namespace duckdb
