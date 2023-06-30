//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/plan_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"

namespace duckdb {

class JoinOrderOptimizer;

struct GenerateJoinRelation {
	GenerateJoinRelation(JoinRelationSet &set, unique_ptr<LogicalOperator> op_p) : set(set), op(std::move(op_p)) {
	}

	JoinRelationSet &set;
	unique_ptr<LogicalOperator> op;
};

class PlanRewriter {
public:
	explicit PlanRewriter(JoinOrderOptimizer *optimizer)
	    : join_optimizer(optimizer) {
	}

private:
	//! Set of all relations considered in the join optimizer
	JoinOrderOptimizer *join_optimizer;

public:
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);

private:
	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
	                                   JoinNode &node);
};

} // namespace duckdb
