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

	//	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
	//	//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	//	bool ExtractJoinRelations(LogicalOperator &input_op, vector<reference<LogicalOperator>> &filter_operators,
	//	                          optional_ptr<LogicalOperator> parent = nullptr);

	//! During the extract join relation phase, we add a relations to our relation map
	//	void AddRelation(optional_ptr<LogicalOperator> &parent, LogicalOperator &input_op,
	//	                 LogicalOperator &data_retreival_op);

	//	//! Emit a pair as a potential join candidate. Returns the best plan found for the (left, right) connection
	//(either
	//	//! the newly created plan, or an existing plan)
	//	JoinNode &EmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);
	//	//! Tries to emit a potential join candidate pair. Returns false if too many pairs have already been emitted,
	//	//! cancelling the dynamic programming step.
	//	bool TryEmitPair(JoinRelationSet &left, JoinRelationSet &right, const vector<reference<NeighborInfo>> &info);

	//! Used for debugging purposes.
	string GetFilterString(unordered_set<idx_t>, string column_name);

	//	bool EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right, unordered_set<idx_t> exclusion_set);
	//	//! Emit a relation set node
	//	bool EmitCSG(JoinRelationSet &node);
	//	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	//	bool EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set);
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);
	//	//! Generate cross product edges inside the side
	//	void GenerateCrossProducts();
	//	//! Perform the join order solving
	//	void SolveJoinOrder();
	//	//! Solve the join order exactly using dynamic programming. Returns true if it was completed successfully (i.e.
	// did
	//	//! not time-out)
	//	bool SolveJoinOrderExactly();
	//	//! Solve the join order approximately using a greedy algorithm
	//	void SolveJoinOrderApproximately();

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode &node);
};

} // namespace duckdb
