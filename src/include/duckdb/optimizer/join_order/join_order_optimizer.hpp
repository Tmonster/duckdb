//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_order_enumerator.hpp"
#include "duckdb/optimizer/join_order/relation_extractor.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/plan_rewriter.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

class JoinOrderOptimizer {
public:
	explicit JoinOrderOptimizer(ClientContext &context)
	    : context(context), cardinality_estimator(context, this), relation_extractor(context, this),
	      join_enumerator(this), plan_rewriter(this) {
	}

	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

private:
	ClientContext &context;

	CardinalityEstimator cardinality_estimator;

	RelationExtractor relation_extractor;

	JoinOrderEnumerator join_enumerator;

	PlanRewriter plan_rewriter;

	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;

	//! A mapping of base table index -> index into relations array (relation_id)
	unordered_map<idx_t, idx_t> relation_mapping;
	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;

	//! The set of filters extracted from the query graph
	vector<unique_ptr<Expression>> filters;
	//! The set of filter infos created from the extracted filters
	vector<unique_ptr<FilterInfo>> filter_infos;

	//! The set of edges used in the join optimizer
	QueryGraph query_graph;

	//! Get column bindings from a filter
	void GetColumnBinding(Expression &expression, ColumnBinding &binding);
};

} // namespace duckdb
