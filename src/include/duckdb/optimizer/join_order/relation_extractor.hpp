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
	    : context(context), join_optimizer(optimizer) {
	}

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	JoinOrderOptimizer *join_optimizer;
	vector<reference<LogicalOperator>> filter_operators;

public:
	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
	//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, optional_ptr<LogicalOperator> parent = nullptr);
	//! Extract the bindings referred to by an Expression
	bool ExtractRelationBindings(Expression &expression, unordered_set<idx_t> &bindings);
	//! Extract the join&SingleColumn filters from the join plan. Join Filters are used to create edges between
	//! the relations
	void ExtractFilters();
	void CreateQueryGraph();

private:
	void GetColumnBinding(Expression &expression, ColumnBinding &binding);
	//! During the extract join relation phase, we add a relations to our relation map
	void AddRelation(optional_ptr<LogicalOperator> &parent, LogicalOperator &input_op,
	                 LogicalOperator &data_retreival_op);

	//! Used for debugging purposes.
	string GetFilterString(unordered_set<idx_t>, string column_name);
};

} // namespace duckdb
