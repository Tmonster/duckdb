#include "duckdb/optimizer/join_order_optimizer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>
#include <iostream>
#include <limits>

namespace duckdb {

using JoinNode = JoinOrderOptimizer::JoinNode;

double DEFAULT_SELECTIVITY = 0.2;

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(unordered_set<T> &a, unordered_set<T> &b) {
	for (auto &entry : a) {
		if (b.find(entry) != b.end()) {
			return false;
		}
	}
	return true;
}

//! Extract the set of relations referred to inside an expression
bool JoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expression;
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		bindings.insert(relation_mapping[colref.binding.table_index]);
	}
	if (expression.type == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	D_ASSERT(expression.type != ExpressionType::SUBQUERY);
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
		if (!ExtractBindings(expr, bindings)) {
			can_reorder = false;
			return;
		}
	});
	return can_reorder;
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
		// we don't, we need to create one
		auto filter = make_unique<LogicalFilter>();
		filter->children.push_back(move(node));
		node = move(filter);
	}
	// push the filter into the LogicalFilter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto filter = (LogicalFilter *)node.get();
	filter->expressions.push_back(move(expr));
	return node;
}

bool JoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                              LogicalOperator *parent) {
	LogicalOperator *op = &input_op;
	while (op->children.size() == 1 && (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
	                                    op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			// extract join conditions from filter
			filter_operators.push_back(op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
		    op->type == LogicalOperatorType::LOGICAL_WINDOW) {
			// don't push filters through projection or aggregate and group by
			JoinOrderOptimizer optimizer(context);
			op->children[0] = optimizer.Optimize(move(op->children[0]));
			return false;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	    op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = (LogicalComparisonJoin &)*op;
		if (join.join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(op);
		} else {
			// non-inner join, not reorderable yet
			non_reorderable_operation = true;
			if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
				// for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
				// we convert to doing a RIGHT OUTER JOIN
				// FIXME: for now we don't swap if the right_projection_map is not empty
				// this can be fixed once we implement the left_projection_map properly...
				auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
				auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
				if (rhs_cardinality > lhs_cardinality * 2) {
					join.join_type = JoinType::RIGHT;
					std::swap(join.children[0], join.children[1]);
					for (auto &cond : join.conditions) {
						std::swap(cond.left, cond.right);
						cond.comparison = FlipComparisionExpression(cond.comparison);
					}
				}
			}
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		for (auto &child : op->children) {
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(move(child));
		}
		// after this we want to treat this node as one  "end node" (like e.g. a base relation)
		// however the join refers to multiple base relations
		// enumerate all base relations obtained from this join and add them to the relation mapping
		// also, we have to resolve the join conditions for the joins here
		// get the left and right bindings
		unordered_set<idx_t> bindings;
		LogicalJoin::GetTableReferences(*op, bindings);
		// now create the relation that refers to all these bindings
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		for (idx_t it : bindings) {
			relation_mapping[it] = relations.size();
		}
		relations.push_back(move(relation));
		return true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		// inner join or cross product
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	} else if (op->type == LogicalOperatorType::LOGICAL_GET) {
		// base table scan, add to set of relations
		auto get = (LogicalGet *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[get->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		// base table scan, add to set of relations
		auto get = (LogicalExpressionGet *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[get->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		// table function call, add to set of relations
		auto dummy_scan = (LogicalDummyScan *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[dummy_scan->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto proj = (LogicalProjection *)op;
		// we run the join order optimizer witin the subquery as well
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(move(op->children[0]));
		// projection, add to the set of relations
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[proj->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	}
	return false;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

static std::string PrintMultiplicities(JoinNode *node) {
	std::string res="[";
	unordered_map<idx_t, double>::iterator it;
	for(it = node->multiplicities.begin(); it != node->multiplicities.end(); it++) {
		res += "{" + std::to_string(it->first) + ":" + std::to_string(it->second) + "},";
	}
	res += "]";
	return res;
}

static std::string PrintSelectivities(JoinNode *node) {
	std::string res="[";
	unordered_map<idx_t, double>::iterator it;
	for(it = node->selectivities.begin(); it != node->selectivities.end(); it++) {
		res += "{" + std::to_string(it->first) + ":" + std::to_string(it->second) + "},";
	}
	res += "]";
	return res;
}

static std::string printFilters(NeighborInfo *info) {
	std::string res = "";
	vector<FilterInfo *>::iterator it;
	for (idx_t it = 0; it < info->filters.size(); it++) {
		res += "filter info " + std::to_string(it) + "\n";
		res += "filter_index = " + std::to_string(info->filters[it]->filter_index) + "\n";
		res += "filter set = " + info->filters[it]->set->ToString() + "\n";
		res += "left set = " + info->filters[it]->left_set->ToString() + "\n";
		res += "right set = " + info->filters[it]->right_set->ToString() + "\n";
	}
	return res;
}

static std::string printJoinRelationSet(JoinRelationSet *set) {
	std::string res = "[";
	for (idx_t it = 0; it < set->count; it++) {
		res += std::to_string(set->relations[it]) + ", ";
	}
	res += "]";
	return res;
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> JoinOrderOptimizer::CreateJoinTree(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, bool switched) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	if (left->cardinality < right->cardinality) {
		return CreateJoinTree(set, info, right, left, true);
	}
	// the expected cardinality is the max of the child cardinalities
	// FIXME: we should obviously use better cardinality estimation here
	// but for now we just assume foreign key joins only
	idx_t expected_cardinality;
	// TODO: determine left filters and right filers.
	if (info->filters.empty()) {
		// cross product
		expected_cardinality = left->cardinality * right->cardinality;
		auto cost = expected_cardinality + left->cost + right->cost;

		auto result = JoinNode(set, info, left, right, expected_cardinality, cost);
		for(idx_t i = 0; i < left->set->count; i++) {
			result.multiplicities[left->set->relations[i]] =  left->multiplicities[left->set->relations[i]] * right->cardinality;
		}
		for(idx_t i = 0; i < right->set->count; i++) {
			result.multiplicities[right->set->relations[i]] =  right->multiplicities[right->set->relations[i]] * left->cardinality;
		}
		return make_unique<JoinNode>(result);
	}

	// normal join, expect foreign key join
	JoinRelationSet *left_join_relations = left->set;
	JoinRelationSet *right_join_relations = right->set;

	// Get underlying operators and initialize selectivities
	idx_t left_relation_id;
	for (idx_t it = 0; it < left_join_relations->count; it++) {
		left_relation_id = left_join_relations->relations[it];
		if (relations.at(left_relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
			auto logical_get = (LogicalGet*)&relations.at(left_relation_id)->op;
			// make sure you don't check each realtion more than once. That could be causing unnecessary overhead.
			if (logical_get->table_filters.has_filters()) {
				left->selectivities[left_relation_id] = DEFAULT_SELECTIVITY;
			}
		}
	}

	idx_t right_relation_id;
	for (idx_t it = 0; it < right_join_relations->count; it++) {
		right_relation_id = right_join_relations->relations[it];
		if (relations.at(left_relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
			auto logical_get = (LogicalGet*)&relations.at(left_relation_id)->op;
			if (logical_get->table_filters.has_filters()) {
				right->selectivities[right_relation_id] = DEFAULT_SELECTIVITY;
			}
		}
	}

	auto right_multiplicity = std::numeric_limits<double>::max();
	auto right_selectivity = std::numeric_limits<double>::max();

	auto left_multiplicity = std::numeric_limits<double>::max();
	auto left_selectivity = std::numeric_limits<double>::max();
	// consider the min values for multiplicity and selectivity as usually join conditions and values.

	double min_left_mult, min_right_mult = std::numeric_limits<double>::max();
	double min_left_sel, min_right_sel = std::numeric_limits<double>::max();
	double min_cardinality_multiplier = std::numeric_limits<double>::max();
	idx_t relation_id_min_left, relation_id_min_right = -1;

	for(idx_t it = 0; it < info->filters.size(); it++) {
		if (JoinRelationSet::IsSubset(right_join_relations, info->filters[it]->left_set) &&
		    JoinRelationSet::IsSubset(left_join_relations, info->filters[it]->right_set)){
			//TODO! add check that there is only one value per filter in a set
			for (idx_t it3 = 0; it3 < info->filters[it]->left_set->count; it3++) {
				right_multiplicity = MinValue(right->multiplicities[info->filters[it]->left_set->relations[it3]], right_multiplicity);
				right_selectivity = MinValue(right->selectivities[info->filters[it]->left_set->relations[it3]], right_selectivity);
				right_relation_id = info->filters[it]->left_set->relations[it3];
			}
			assert(info->filters[it]->right_set->count > 0);
			for (idx_t it3 = 0; it3 < info->filters[it]->right_set->count; it3++) {
				left_multiplicity = MinValue(left->multiplicities[info->filters[it]->right_set->relations[it3]], left_multiplicity);
				left_selectivity = MinValue(left->selectivities[info->filters[it]->right_set->relations[it3]], left_selectivity);
				left_relation_id = info->filters[it]->right_set->relations[it3];
				if (right_multiplicity * right_selectivity < min_cardinality_multiplier) {
					min_left_mult = left_multiplicity;
					min_left_sel = left_selectivity;
					min_right_mult = right_multiplicity;
					min_right_sel = right_selectivity;
					relation_id_min_left = left_relation_id;
					relation_id_min_right = right_relation_id;
				}
			}
		}
		// currently finding in multiplicities because the syntax is easier for me.
		else if (JoinRelationSet::IsSubset(left_join_relations, info->filters[it]->left_set) &&
		         JoinRelationSet::IsSubset(right_join_relations, info->filters[it]->right_set)) {
			assert(info->filters[it]->right_set->count > 0);
			for (idx_t it3 = 0; it3 < info->filters[it]->right_set->count; it3++) {
				right_multiplicity =
				    MinValue(right->multiplicities[info->filters[it]->right_set->relations[it3]], right_multiplicity);
				right_selectivity =
				    MinValue(right->selectivities[info->filters[it]->right_set->relations[it3]], right_selectivity);
				right_relation_id = info->filters[it]->right_set->relations[it3];
			}
			assert(info->filters[it]->left_set->count > 0);
			for (idx_t it3 = 0; it3 < info->filters[it]->left_set->count; it3++) {
				left_multiplicity =
					MinValue(left->multiplicities[info->filters[it]->left_set->relations[it3]], left_multiplicity);
				left_selectivity =
					MinValue(left->selectivities[info->filters[it]->left_set->relations[it3]], left_selectivity);
				left_relation_id = info->filters[it]->left_set->relations[it3];
				if (right_multiplicity * right_selectivity < min_cardinality_multiplier) {
					min_left_mult = left_multiplicity;
					min_left_sel = left_selectivity;
					min_right_mult = right_multiplicity;
					min_right_sel = right_selectivity;
					relation_id_min_left = left_relation_id;
					relation_id_min_right = right_relation_id;
				}
			}
		} else {
			std::cout << "left filters are not subset of left join relations, and right filters are not subset of right join relations" << std::endl;
			std::cout << "OR" << std::endl;
			std::cout << "left filters are subset of right join relations, and right filters are not subset of left join relations" << std::endl;
			std::cout << "right join relations = " << printJoinRelationSet(right_join_relations) << std::endl;
			std::cout << "left join relations = " << printJoinRelationSet(left_join_relations) << std::endl;
			std::cout << printFilters(info) << std::endl;
		}
	}

//	// this technically should never happen as the right and left multiplicities should decrease
//	// when iterating through the filters.
	if (left_multiplicity == std::numeric_limits<double>::max()) left_multiplicity = 1;
	if (right_multiplicity == std::numeric_limits<double>::max()) right_multiplicity = 1;
	if (left_selectivity == std::numeric_limits<double>::max()) left_selectivity = 1;
	if (right_selectivity == std::numeric_limits<double>::max()) right_multiplicity = 1;
	if (min_right_mult == std::numeric_limits<double>::max()) min_right_mult = 1;
	if (min_right_sel == std::numeric_limits<double>::max()) min_right_sel = 1;
	if (min_left_mult == std::numeric_limits<double>::max()) min_left_mult = 1;
	if (min_left_sel == std::numeric_limits<double>::max()) min_left_sel = 1;


	// TODO: check this, make sure this is ideal
	// Left cardinality is always the largest. Then take max multiplicity of left and right.
	//! 1) new cardinality estimation
//	if (min_right_sel < 1) {
//		std::cout << "min_right_sel = " << min_right_sel << std::endl;
//	}
	expected_cardinality = left->cardinality * min_right_sel * min_right_mult;

	// cost is expected_cardinality plus the cost of the previous plans
	auto cost = expected_cardinality + left->cost + right->cost;
	// create result and add multiplicities
	auto result = JoinNode(set, info, left, right, expected_cardinality, cost);

	//! 2) update result mult for right relation.
	if (left->multiplicities[relation_id_min_left] == 1) {
		result.multiplicities[relation_id_min_right] =
			MaxValue(1.0, right->multiplicities[relation_id_min_right] * (left->cardinality / right->cardinality));
	} else {
		result.multiplicities[relation_id_min_right] = MaxValue(
			1.0, right->multiplicities[relation_id_min_right] * left->multiplicities[relation_id_min_left]);
	}

	//! 3) update the rest of the right relations
	auto right_default_mult = left->cardinality / right->cardinality;
	for(idx_t i = 0; i < right->set->count; i++) {
		result.selectivities[right->set->relations[i]] = right->selectivities[right->set->relations[i]];
		if (left->multiplicities[relation_id_min_left] == 1) {
			result.multiplicities[right->set->relations[i]] = right_default_mult;
		} else {
			result.multiplicities[right->set->relations[i]] =
			    min_left_mult * right->multiplicities[right->set->relations[i]];
		}
	}

	//! 4) update the left multiplicities
	result.multiplicities[relation_id_min_left] = result.multiplicities[relation_id_min_right];

    //! 5) update on left multiplicities.
	for(idx_t i = 0; i < left->set->count; i++) {
		result.selectivities[left->set->relations[i]] = left->selectivities[left->set->relations[i]] * min_right_sel;
		if (left->set->relations[i] == relation_id_min_left) {
			continue;
		}
		result.multiplicities[left->set->relations[i]] = min_right_mult * left->multiplicities[left->set->relations[i]];
	}

	return make_unique<JoinNode>(result);
}

JoinNode *JoinOrderOptimizer::EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info) {
	// get the left and right join plans
	auto &left_plan = plans[left];
	auto &right_plan = plans[right];
	auto new_set = set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, left_plan.get(), right_plan.get());
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	if (entry == plans.end() || new_plan->cost < entry->second->cost) {
		// the plan is the optimal plan, move it into the dynamic programming tree
		auto result = new_plan.get();
		plans[new_set] = move(new_plan);
		return result;
	}
	return entry->second.get();
}

bool JoinOrderOptimizer::TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info) {
	pairs++;
	if (pairs >= 2000) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool JoinOrderOptimizer::EmitCSG(JoinRelationSet *node) {
	if (node->count == relations.size()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node->relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	// we iterate over the neighbors ordered by their first node
	sort(neighbors.begin(), neighbors.end());
	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto neighbor_relation = set_manager.GetJoinRelation(neighbor);
		auto connection = query_graph.GetConnection(node, neighbor_relation);
		if (connection) {
			if (!TryEmitPair(node, neighbor_relation, connection)) {
				return false;
			}
		}
		if (!EnumerateCmpRecursive(node, neighbor_relation, exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right,
                                               unordered_set<idx_t> exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto combined_set = set_manager.Union(right, neighbor);
		if (combined_set->count > right->count && plans.find(combined_set) != plans.end()) {
			auto connection = query_graph.GetConnection(left, combined_set);
			if (connection) {
				if (!TryEmitPair(left, combined_set, connection)) {
					return false;
				}
			}
		}
		union_sets[i] = combined_set;
	}
	// recursively enumerate the sets
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		new_exclusion_set.insert(neighbors[i]);
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exlusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	// now first emit the connected subgraphs of the neighbors
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto new_set = set_manager.Union(node, neighbor);
		if (new_set->count > node->count && plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets[i] = new_set;
	}
	// recursively enumerate the sets
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		new_exclusion_set.insert(neighbors[i]);
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = relations.size(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto start_node = set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i - 1; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

void JoinOrderOptimizer::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<JoinRelationSet *> join_relations; // T in the paper
	for (idx_t i = 0; i < relations.size(); i++) {
		join_relations.push_back(set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		JoinNode *best_connection = nullptr;
		for (idx_t i = 0; i < join_relations.size(); i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnection(left, right);
				if (connection) {
					// we can! check the cost of this connection
					auto node = EmitPair(left, right, connection);
					if (!best_connection || node->cost < best_connection->cost) {
						// best pair found so far
						best_connection = node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			JoinNode *smallest_plans[2] = {nullptr};
			idx_t smallest_index[2];
			for (idx_t i = 0; i < join_relations.size(); i++) {
				// get the plan for this relation
				auto current_plan = plans[join_relations[i]].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] || smallest_plans[j]->cardinality > current_plan->cardinality) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			if (!smallest_plans[0] || !smallest_plans[1]) {
				throw InternalException("Internal error in join order optimizer");
			}
			D_ASSERT(smallest_plans[0] && smallest_plans[1]);
			D_ASSERT(smallest_index[0] != smallest_index[1]);
			auto left = smallest_plans[0]->set;
			auto right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph.CreateEdge(left, right, nullptr);
			// now emit the pair and continue with the algorithm
			auto connection = query_graph.GetConnection(left, right);
			D_ASSERT(connection);

			best_connection = EmitPair(left, right, connection);
			best_left = smallest_index[0];
			best_right = smallest_index[1];
			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				std::swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		D_ASSERT(best_right > best_left);
		join_relations.erase(join_relations.begin() + best_right);
		join_relations.erase(join_relations.begin() + best_left);
		join_relations.push_back(best_connection->set);
	}
}

void JoinOrderOptimizer::SolveJoinOrder() {
	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}
}

void JoinOrderOptimizer::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < relations.size(); i++) {
		auto left = set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < relations.size(); j++) {
			if (i != j) {
				auto right = set_manager.GetJoinRelation(j);
				query_graph.CreateEdge(left, right, nullptr);
				query_graph.CreateEdge(right, left, nullptr);
			}
		}
	}
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == rel.op) {
			// found it! take ownership of it from the parent
			auto result = move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node) {
	JoinRelationSet *left_node = nullptr, *right_node = nullptr;
	JoinRelationSet *result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node->left && node->right) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left);
		auto right = GenerateJoins(extracted_relations, node->right);

		if (node->info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(move(left.second), move(right.second));
		} else {
			// we have filters, create a join node
			auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(move(left.second));
			join->children.push_back(move(right.second));
			// set the join conditions from the join node
			for (auto &f : node->info->filters) {
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters[f->filter_index]);
				auto condition = move(filters[f->filter_index]);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(left.first, f->left_set) &&
				          JoinRelationSet::IsSubset(right.first, f->right_set)) ||
				         (JoinRelationSet::IsSubset(left.first, f->right_set) &&
				          JoinRelationSet::IsSubset(right.first, f->left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = (BoundComparisonExpression &)*condition;
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(left.first, f->left_set);
				cond.left = !invert ? move(comparison.left) : move(comparison.right);
				cond.right = !invert ? move(comparison.right) : move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(cond.comparison);
				}
				join->conditions.push_back(move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = move(join);
		}
		left_node = left.first;
		right_node = right.first;
		result_relation = set_manager.Union(left_node, right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node->set->count == 1);
		D_ASSERT(extracted_relations[node->set->relations[0]]);
		result_relation = node->set;
		result_operator = move(extracted_relations[node->set->relations[0]]);
	}

	result_operator->estimated_cardinality = node->cardinality;

	/* START */
	/* used when printing out join trees, can later be removed */
	result_operator->cost = node->cost;
	result_operator->max_mult = 0;
	std::unordered_map<idx_t, double>::const_iterator it = node->multiplicities.begin();
	for(; it != node->multiplicities.end(); it++) {
		if (result_operator->max_mult < node->multiplicities[it->first]) {
			result_operator->max_mult = node->multiplicities[it->first];
		};
	}
	result_operator->min_sel = 1000000000;
	std::unordered_map<idx_t, double>::const_iterator it2 = node->selectivities.begin();
	for(; it2 != node->multiplicities.end(); it2++) {
		if (result_operator->min_sel > node->selectivities[it2->first]) {
			result_operator->min_sel = node->selectivities[it2->first];
		};
	}
	/* END */

	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filter_infos) {
		// check if the filter has already been extracted
		auto info = filter_info.get();
		if (filters[info->filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info->set->count > 0 && JoinRelationSet::IsSubset(result_relation, info->set)) {
				auto filter = move(filters[info->filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info->left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(move(result_operator), move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(left_node, info->left_set) &&
				    JoinRelationSet::IsSubset(right_node, info->right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(right_node, info->left_set) &&
				           JoinRelationSet::IsSubset(left_node, info->right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(move(result_operator), move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = (BoundComparisonExpression &)*filter;
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? move(comparison.left) : move(comparison.right);
				cond.right = !invert ? move(comparison.right) : move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(move(node->children[0]));
					comp_join->children.push_back(move(node->children[1]));
					comp_join->conditions.push_back(move(cond));
					if (node == result_operator.get()) {
						result_operator = move(comp_join);
					} else {
						D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
						result_operator->children[0] = move(comp_join);
					}
				} else {
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
					auto &comp_join = (LogicalComparisonJoin &)*node;
					comp_join.conditions.push_back(move(cond));
				}
			}
		}
	}
	return make_pair(result_relation, move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	for (auto &relation : relations) {
		extracted_relations.push_back(ExtractJoinRelation(*relation));
	}
	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : filters) {
		// check if the filter has already been extracted
		if (filter) {
			// if not we need to push it
			join_tree.second = PushFilter(move(join_tree.second), move(filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		//		std::cout << join_tree.second->ToString() << std::endl;
		return move(join_tree.second);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	//	std::cout << join_tree.second->ToString() << std::endl;
	parent->children[0] = move(join_tree.second);
	return plan;
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	D_ASSERT(filters.empty() && relations.empty()); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();
	// now we optimize the current plan
	// we skip past until we find the first projection, we do this because the HAVING clause inserts a Filter AFTER the
	// group by and this filter cannot be reordered
	// extract a list of all relations that have to be joined together
	// and a list of all conditions that is applied to them
	vector<LogicalOperator *> filter_operators;
	if (!ExtractJoinRelations(*op, filter_operators)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	expression_set_t filter_set;
	for (auto &op : filter_operators) {
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = (LogicalComparisonJoin &)*op;
			D_ASSERT(join.join_type == JoinType::INNER);
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_unique<BoundComparisonExpression>(cond.comparison, move(cond.left), move(cond.right));
				if (filter_set.find(comparison.get()) == filter_set.end()) {
					filter_set.insert(comparison.get());
					filters.push_back(move(comparison));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : op->expressions) {
				if (filter_set.find(expression.get()) == filter_set.end()) {
					filter_set.insert(expression.get());
					filters.push_back(move(expression));
				}
			}
			op->expressions.clear();
		}
	}
	// create potential edges from the comparisons
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		auto info = make_unique<FilterInfo>();
		auto filter_info = info.get();
		filter_infos.push_back(move(info));
		// first extract the relation set for the entire filter
		unordered_set<idx_t> bindings;
		ExtractBindings(*filter, bindings);
		filter_info->set = set_manager.GetJoinRelation(bindings);
		filter_info->filter_index = i;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto comparison = (BoundComparisonExpression *)filter.get();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<idx_t> left_bindings, right_bindings;
			ExtractBindings(*comparison->left, left_bindings);
			ExtractBindings(*comparison->right, right_bindings);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = set_manager.GetJoinRelation(left_bindings);
				filter_info->right_set = set_manager.GetJoinRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(filter_info->left_set, filter_info->right_set, filter_info);
						query_graph.CreateEdge(filter_info->right_set, filter_info->left_set, filter_info);
					} else {
						continue;
					}
					continue;
				}
			}
		}
	}
	// now use dynamic programming to figure out the optimal join order
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &rel = *relations[i];
		auto node = set_manager.GetJoinRelation(i);
		plans[node] = make_unique<JoinNode>(node, rel.op->EstimateCardinality(context));
	}
	// now we perform the actual dynamic programming to compute the final result
	SolveJoinOrder();
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < relations.size(); i++) {
		bindings.insert(i);
	}
	auto total_relation = set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (context.config.force_no_cross_product) {
			throw InternalException("HyperGraph isn't connected");
		}
		GenerateCrossProducts();
		//! solve the join order again
		SolveJoinOrder();
		// now we can obtain the final plan!
		final_plan = plans.find(total_relation);
		D_ASSERT(final_plan != plans.end());
	}
	// now perform the actual reordering
	return RewritePlan(move(plan), final_plan->second.get());
}

} // namespace duckdb
