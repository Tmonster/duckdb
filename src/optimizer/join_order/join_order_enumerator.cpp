#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/join_order_enumerator.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <algorithm>
#include <cmath>

namespace std {

//! A JoinNode is defined by the relations it joins.
template <>
struct hash<duckdb::JoinNode> {
	inline string operator()(const duckdb::JoinNode &join_node) const {
		return join_node.set.ToString();
	}
};
} // namespace std

namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise

bool JoinOrderEnumerator::NodeInFullPlan(JoinNode &node) {
	return join_nodes_in_full_plan.find(node.set.ToString()) != join_nodes_in_full_plan.end();
}

void JoinOrderEnumerator::UpdateJoinNodesInFullPlan(JoinNode &node) {
	if (node.set.count == set_manager->GetRoot().count) {
		join_nodes_in_full_plan.clear();
	}
	if (node.set.count < set_manager->GetRoot().count) {
		join_nodes_in_full_plan.insert(node.set.ToString());
	}
	if (node.left) {
		UpdateJoinNodesInFullPlan(*node.left);
	}
	if (node.right) {
		UpdateJoinNodesInFullPlan(*node.right);
	}
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> JoinOrderEnumerator::CreateJoinTree(JoinRelationSet &set,
                                                         const vector<reference<NeighborInfo>> &possible_connections,
                                                         JoinNode &left, JoinNode &right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	double expected_cardinality;
	optional_ptr<NeighborInfo> best_connection;
	auto plan = plans.find(&set);
	// if we have already calculated an expected cardinality for this set,
	// just re-use that cardinality
	if (left.GetCardinality<double>() < right.GetCardinality<double>()) {
		return CreateJoinTree(set, possible_connections, right, left);
	}

	if (!possible_connections.empty()) {
		best_connection = &possible_connections.back().get();
	}

	double cost = 0;
	if (plan != plans.end()) {
		if (!plan->second) {
			throw InternalException("No plan: internal error in join order optimizer");
		}
	}
	cost = join_cost_model.compute_cost(left, right);


	auto result = make_uniq<JoinNode>(set, best_connection, left, right, expected_cardinality, cost);
	D_ASSERT(cost >= expected_cardinality);
	return result;
}

void JoinOrderEnumerator::Init(QueryGraph &query_graph_, vector<unique_ptr<SingleJoinRelation>> &relations_,
                                     JoinRelationSetManager &set_manager_) {
	query_graph = &query_graph_;
	set_manager = &set_manager_;
	node_ops = join_cost_model.Init(relations_, set_manager_);

	// TODO: clean this up. This is really disgusting code.
	for (idx_t i = 0; i < relations_.size(); i++) {
		auto &rel = *relations_[i];
		auto &node = set_manager->GetJoinRelation(i);
		plans[&node] = make_uniq<JoinNode>(node, 0);
	}
}

JoinNode &JoinOrderEnumerator::EmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                        const vector<reference<NeighborInfo>> &info) {
	// get the left and right join plans
	auto &left_plan = plans[&left];
	auto &right_plan = plans[&right];
	if (!left_plan || !right_plan) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	auto &new_set = set_manager->Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, *left_plan, *right_plan);
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(&new_set);
	if (entry == plans.end() || new_plan->GetCost() < entry->second->GetCost()) {
		// the plan is the optimal plan, move it into the dynamic programming tree
		auto &result = *new_plan;

		//! make sure plans are symmetric for cardinality estimation
		// TODO verify that we actually have two different calculations.
		if (entry != plans.end()) {
			join_cost_model.VerifySymmetry(result, *entry->second);
		}
		if (full_plan_found &&
		    join_nodes_in_full_plan.find(new_plan->set.ToString()) != join_nodes_in_full_plan.end()) {
			must_update_full_plan = true;
		}
		// check for all relations. JoinRelationSetManager should be able to handle this.
		if (new_set.count == set_manager->GetRoot().count) {
			full_plan_found = true;
			// If we find a full plan, we need to keep track of which nodes are in the full plan.
			// It's possible the DP algorithm updates one of these nodes, then goes on to solve
			// the order approximately. In the approximate algorithm, it's not guaranteed that the
			// node references are updated. If the original full plan is determined to still have
			// the lowest cost, it's possible to get use-after-free errors.
			// If we know a node in the full plan is updated, we can prevent ourselves from exiting the
			// DP algorithm until the last plan updated is a full plan
			UpdateJoinNodesInFullPlan(result);
			if (must_update_full_plan) {
				must_update_full_plan = false;
			}
		}

		D_ASSERT(new_plan);
		plans[&new_set] = std::move(new_plan);
		return result;
	}
	return *entry->second;
}

bool JoinOrderEnumerator::TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                      const vector<reference<NeighborInfo>> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000 && !must_update_full_plan) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node.count; i++) {
		exclusion_set.insert(node.relations[i]);
	}
}

bool JoinOrderEnumerator::EmitCSG(JoinRelationSet &node) {
	if (node.count == set_manager->GetRoot().count) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node.relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph->GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater_equal<idx_t>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] >= neighbors[i + 1]);
	}
	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto &neighbor_relation = set_manager->GetJoinRelation(neighbor);
		auto connections = query_graph->GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}
		if (!EnumerateCmpRecursive(node, neighbor_relation, exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderEnumerator::EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
                                                unordered_set<idx_t> &exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph->GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto &neighbor = set_manager->GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto &combined_set = set_manager->Union(right, neighbor);
		if (combined_set.count > right.count && plans.find(&combined_set) != plans.end()) {
			auto connections = query_graph->GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined_set);
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

bool JoinOrderEnumerator::EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph->GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto &neighbor = set_manager->GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto &new_set = set_manager->Union(node, neighbor);
		if (new_set.count > node.count && plans.find(&new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}
	// recursively enumerate the sets
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// Reset the exclusion set so that the algorithm considers all combinations
		// of the exclusion_set with a subset of neighbors.
		new_exclusion_set = exclusion_set;
		new_exclusion_set.insert(neighbors[i]);
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderEnumerator::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = set_manager->GetRoot().count; i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto &start_node = set_manager->GetJoinRelation(i - 1);
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

static vector<unordered_set<idx_t>> AddSuperSets(vector<unordered_set<idx_t>> current,
                                                 const vector<idx_t> &all_neighbors) {
	vector<unordered_set<idx_t>> ret;
	for (auto &neighbor : all_neighbors) {
		for (auto &neighbor_set : current) {
			auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<idx_t> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(new_set);
			}
		}
	}
	return ret;
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<idx_t>> GetAllNeighborSets(unordered_set<idx_t> &exclusion_set, vector<idx_t> neighbors) {
	vector<unordered_set<idx_t>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<idx_t>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<idx_t>({neighbor}));
		ret.push_back(unordered_set<idx_t>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
#if DEBUG
	// drive by test to make sure we have an accurate amount of
	// subsets, and that each neighbor is in a correct amount
	// of those subsets.
	D_ASSERT(ret.size() == pow(2, neighbors.size()) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == pow(2, neighbors.size() - 1));
	}
#endif
	return ret;
}

void JoinOrderEnumerator::UpdateDPTree(JoinNode &new_plan) {
	if (!NodeInFullPlan(new_plan)) {
		// if the new node is not in the full plan, feel free to return
		// because you won't be updating the full plan.
		return;
	}
	auto &new_set = new_plan.set;
	// now update every plan that uses this plan
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < new_set.count; i++) {
		exclusion_set.insert(new_set.relations[i]);
	}
	auto neighbors = query_graph->GetNeighbors(new_set, exclusion_set);
	auto all_neighbors = GetAllNeighborSets(exclusion_set, neighbors);
	for (auto neighbor : all_neighbors) {
		auto &neighbor_relation = set_manager->GetJoinRelation(neighbor);
		auto &combined_set = set_manager->Union(new_set, neighbor_relation);

		auto combined_set_plan = plans.find(&combined_set);
		if (combined_set_plan == plans.end()) {
			continue;
		}

		double combined_set_plan_cost = combined_set_plan->second->GetCost();
		auto connections = query_graph->GetConnections(new_set, neighbor_relation);
		// recurse and update up the tree if the combined set produces a plan with a lower cost
		// only recurse on neighbor relations that have plans.
		auto right_plan = plans.find(&neighbor_relation);
		if (right_plan == plans.end()) {
			continue;
		}
		auto &updated_plan = EmitPair(new_set, neighbor_relation, connections);
		// <= because the child node has already been replaced. You need to
		// replace the parent node as well in this case
		if (updated_plan.GetCost() < combined_set_plan_cost) {
			UpdateDPTree(updated_plan);
		}
	}
}

void JoinOrderEnumerator::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < set_manager->GetRoot().count; i++) {
		join_relations.push_back(set_manager->GetJoinRelation(i));
	}
	while (set_manager->GetRoot().count > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		optional_ptr<JoinNode> best_connection;
		for (idx_t i = 0; i < set_manager->GetRoot().count; i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < set_manager->GetRoot().count; j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph->GetConnections(left, right);
				if (!connection.empty()) {
					// we can check the cost of this connection
					auto &node = EmitPair(left, right, connection);

					// update the DP tree in case a plan created by the DP algorithm uses the node
					// that was potentially just updated by EmitPair. You will get a use-after-free
					// error if future plans rely on the old node that was just replaced.
					// if node in FullPath, then updateDP tree.
					UpdateDPTree(node);

					if (!best_connection || node.GetCost() < best_connection->GetCost()) {
						// best pair found so far
						best_connection = &node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			optional_ptr<JoinNode> smallest_plans[2];
			idx_t smallest_index[2];
			D_ASSERT(set_manager->GetRoot().count >= 2);

			// first just add the first two join relations. It doesn't matter the cost as the JOO
			// will swap them on estimated cardinality anyway.
			for (idx_t i = 0; i < 2; i++) {
				auto current_plan = plans[&join_relations[i].get()].get();
				smallest_plans[i] = current_plan;
				smallest_index[i] = i;
			}

			// if there are any other join relations that don't have connections
			// add them if they have lower estimated cardinality.
			for (idx_t i = 2; i < set_manager->GetRoot().count; i++) {
				// get the plan for this relation
				auto current_plan = plans[&join_relations[i].get()].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] ||
					    smallest_plans[j]->GetCardinality<double>() > current_plan->GetCardinality<double>()) {
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
			auto &left = smallest_plans[0]->set;
			auto &right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph->CreateEdge(left, right, nullptr);
			// now emit the pair and continue with the algorithm
			auto connections = query_graph->GetConnections(left, right);
			D_ASSERT(!connections.empty());

			best_connection = &EmitPair(left, right, connections);
			best_left = smallest_index[0];
			best_right = smallest_index[1];

			UpdateDPTree(*best_connection);
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

unique_ptr<JoinNode> JoinOrderEnumerator::SolveJoinOrder(bool force_no_cross_product) {
	// TODO: shove the below code into the join enumerator init code.
	// the cardinality estimator initializes the leaf join nodes with estimated cardinalities based on
	// certain table filters.

	// now use dynamic programming to figure out the optimal join order
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree
	// NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.


	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < set_manager->GetRoot().count; i++) {
		bindings.insert(i);
	}
	auto &total_relation = set_manager->GetJoinRelation(bindings);
	auto final_plan = plans.find(&total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		GenerateCrossProducts();
		//! solve the join order again, returning the final plan
		return SolveJoinOrder(force_no_cross_product);
	}
	return std::move(final_plan->second);
}

void JoinOrderEnumerator::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < set_manager->GetRoot().count; i++) {
		auto &left = set_manager->GetJoinRelation(i);
		for (idx_t j = 0; j < set_manager->GetRoot().count; j++) {
			if (i != j) {
				auto &right = set_manager->GetJoinRelation(j);
				query_graph->CreateEdge(left, right, nullptr);
				query_graph->CreateEdge(right, left, nullptr);
			}
		}
	}
}

} // namespace duckdb
