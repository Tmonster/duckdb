//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include <map>


#include <functional>

namespace duckdb {

class JoinOrderOptimizer {
public:
	//! Represents a node in the join plan
	struct JoinNode {
		JoinRelationSet *set;
		NeighborInfo *info;
		idx_t cardinality;
		idx_t cost;
		JoinNode *left;
		JoinNode *right;

		//! have the multiplicity and selectivity stats been initialized?
		bool init_stats;
		bool created_as_intermediate;

		// first idx is a 32 bits for table id
		//                32 bits for column id
		// you need to use some bit manipulation to get he numbers
		unordered_map<idx_t, unordered_set<idx_t>> tab_cols;

		unordered_map<idx_t, idx_t> table_col_mults;
		unordered_map<idx_t, idx_t> table_col_sels;




		//! Create a leaf node in the join tree
		//! set cost to 0 because leaf nodes/base table already exist
		//! cost will be the cost to *produce* an intermediate table
		JoinNode(JoinRelationSet *set, idx_t cardinality)
		    : set(set), info(nullptr), cardinality(cardinality), cost(0), left(nullptr), right(nullptr),
		      init_stats(false), created_as_intermediate(false) {
		}
		//! Create an intermediate node in the join tree
		JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, idx_t cardinality,
		         idx_t cost)
		    : set(set), info(info), cardinality(cardinality), cost(cost), left(left), right(right), init_stats(false), created_as_intermediate(true) {
		}
	};

public:
	explicit JoinOrderOptimizer(ClientContext &context) : context(context) {
	}

	//! Perform join reordering inside a plan
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	unique_ptr<JoinNode> CreateJoinTree(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right,
	                                    bool switched = false);

private:
	ClientContext &context;
	//! The total amount of join pairs that have been considered
	idx_t pairs = 0;
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<idx_t, idx_t> relation_mapping;
	//! A mapping of base table index -> all columns used to determine the join order
	unordered_map<idx_t, unordered_set<idx_t>> relation_to_columns;

	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;
	//! The set of edges used in the join optimizer
	QueryGraph query_graph;
	//! The optimal join plan found for the specific JoinRelationSet*
	unordered_map<JoinRelationSet *, unique_ptr<JoinNode>> plans;
	//! The set of filters extracted from the query graph
	vector<unique_ptr<Expression>> filters;
	//! The set of filter infos created from the extracted filters
	vector<unique_ptr<FilterInfo>> filter_infos;
	//! A map of all expressions a given expression has to be equivalent to. This is used to add "implied join edges".
	//! i.e. in the join A=B AND B=C, the equivalence set of {B} is {A, C}, thus we can add an implied join edge {A <->
	//! C}
	expression_map_t<vector<FilterInfo *>> equivalence_sets;

	//! Extract the bindings referred to by an Expression
	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);

	//! Get column bindings from a filter
	void GetColumnBindings(Expression &expression, pair<idx_t, idx_t> *left_binding, pair<idx_t, idx_t> *right_binding);

	//! initialize the column stats for a node. Used by leaf nodes. Intermediate nodes should not be calling this function
	//! Intermediate node stats are defined when the node is created (in CreateJoinTree)
	void InitColumnStats(JoinNode *node);

	//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
	//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
	bool ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
	                          LogicalOperator *parent = nullptr);
	//! Emit a pair as a potential join candidate. Returns the best plan found for the (left, right) connection (either
	//! the newly created plan, or an existing plan)
	JoinNode *EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);
	//! Tries to emit a potential join candidate pair. Returns false if too many pairs have already been emitted,
	//! cancelling the dynamic programming step.
	bool TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);

	bool EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> exclusion_set);
	//! Emit a relation set node
	bool EmitCSG(JoinRelationSet *node);
	//! Enumerate the possible connected subgraphs that can be joined together in the join graph
	bool EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set);
	//! Rewrite a logical query plan given the join plan
	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node);
	//! Generate cross product edges inside the side
	void GenerateCrossProducts();
	//! Perform the join order solving
	void SolveJoinOrder();
	//! Solve the join order exactly using dynamic programming. Returns true if it was completed successfully (i.e. did
	//! not time-out)
	bool SolveJoinOrderExactly();
	//! Solve the join order approximately using a greedy algorithm
	void SolveJoinOrderApproximately();

	unique_ptr<LogicalOperator> ResolveJoinConditions(unique_ptr<LogicalOperator> op);
	std::pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
	GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node);
};

} // namespace duckdb
