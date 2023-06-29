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
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

struct GenerateJoinRelation {
	GenerateJoinRelation(JoinRelationSet &set, unique_ptr<LogicalOperator> op_p) : set(set), op(std::move(op_p)) {
	}

	JoinRelationSet &set;
	unique_ptr<LogicalOperator> op;
};

class JoinOrderOptimizer {
public:
	explicit JoinOrderOptimizer(ClientContext &context)
	    : context(context), cardinality_estimator(context, this), relation_extractor(context, this),
	      join_enumerator(this) {
	}

	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	unique_ptr<JoinNode> CreateJoinTree(JoinRelationSet &set,
	                                    const vector<reference<NeighborInfo>> &possible_connections, JoinNode &left,
	                                    JoinNode &right);

private:
	ClientContext &context;
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

	friend CardinalityEstimator;
	CardinalityEstimator cardinality_estimator;
	friend RelationExtractor;
	RelationExtractor relation_extractor;
	friend JoinOrderEnumerator;
	JoinOrderEnumerator join_enumerator;

	//! Extract the bindings referred to by an Expression
	bool ExtractRelationBindings(Expression &expression, unordered_set<idx_t> &bindings);

	//! Get column bindings from a filter
	void GetColumnBinding(Expression &expression, ColumnBinding &binding);

	//! Used for debugging purposes.
	string GetFilterString(unordered_set<idx_t>, string column_name);
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);
	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode &node);
};

} // namespace duckdb
