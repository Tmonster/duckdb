//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation_extractor.hpp
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

class RelationExtractor {
public:
	explicit RelationExtractor(ClientContext &context, JoinOrderOptimizer *optimizer)
	    : context(context) {
	}

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	vector<reference<LogicalOperator>> filter_operators;

	//! A mapping of base table index -> index into relations array (relation_id)
	//! Used to make edges in the query graph.
	unordered_map<idx_t, idx_t> relation_mapping;

	//! Set manager
	JoinRelationSetManager set_manager;

	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;
	vector<unique_ptr<Expression>> filters;

public:
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
	//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, optional_ptr<LogicalOperator> parent = nullptr);
	//! Extract the bindings referred to by an Expression
	bool ExtractRelationBindings(Expression &expression, unordered_set<idx_t> &bindings);
	//! Extract the join&SingleColumn filters from the join plan. Join Filters are used to create edges between
	//! the relations
	void ExtractFilters();
	const QueryGraph CreateQueryGraph();

	vector<unique_ptr<SingleJoinRelation>> GetRelations();
	vector<unique_ptr<Expression>> GetFilters();
	JoinRelationSetManager &GetSetManager();
	bool FiltersEmpty();
	bool RelationsEmpty();

private:
	void GetColumnBinding(Expression &expression, ColumnBinding &binding);
	//! During the extract join relation phase, we add a relations to our relation map
	void AddRelation(optional_ptr<LogicalOperator> &parent, LogicalOperator &input_op,
	                 LogicalOperator &data_retreival_op);

	//! Used for debugging purposes.
	string GetFilterString(unordered_set<idx_t>, string column_name);
};

} // namespace duckdb
