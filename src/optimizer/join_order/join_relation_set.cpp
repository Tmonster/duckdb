#include "duckdb/optimizer/join_order/join_relation.hpp"

#include <algorithm>

namespace duckdb {

// LCOV_EXCL_START
string JoinRelationSet::ToString() const {
	string result = "[";
	EnumerateRelations(relations, [&](idx_t relation) { result += to_string(relation) + ", "; });
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

//! Returns true if sub is a subset of super
// bool JoinRelationSetOld::IsSubset(JoinRelationSetOld &super, JoinRelationSetOld &sub) {
// 	D_ASSERT(sub.count > 0);
// 	if (sub.count > super.count) {
// 		return false;
// 	}
// 	idx_t j = 0;
// 	for (idx_t i = 0; i < super.count; i++) {
// 		if (sub.relations[j] == super.relations[i]) {
// 			j++;
// 			if (j == sub.count) {
// 				return true;
// 			}
// 		}
// 	}
// 	return false;
// }

bool JoinRelationSet::IsSubset(JoinRelationSet &super, JoinRelationSet &sub) {
	std::bitset<12> sub_copy = sub.relations;
	sub_copy &= super.relations;
	return sub_copy == sub.relations;
}

void JoinRelationSet::EnumerateRelations(std::bitset<12> relations,
                                         const std::function<void(idx_t relation)> &callback) {
	for (idx_t i = 0; i < PlanEnumerator::THRESHOLD_TO_SWAP_TO_APPROXIMATE; i++) {
		if (relations[i]) {
			callback(i);
		}
	}
}

idx_t JoinRelationSet::Count() const {
	idx_t count = 0;
	for (idx_t i = 0; i < PlanEnumerator::THRESHOLD_TO_SWAP_TO_APPROXIMATE; i++) {
		if (relations[i]) {
			count++;
		}
	}
	return count;
}

idx_t JoinRelationSet::NextNeighbor(idx_t i) {
	for (idx_t j = 0; j < i; j++) {
		if (relations[j]) {
			return j;
		}
	}
	return DConstants::INVALID_INDEX;
}

JoinRelationSet JoinRelationSet::Copy() const {
	JoinRelationSet result;
	result.relations = relations;
	return result;
}

reference<JoinRelationSet> JoinRelationSetManager::GetJoinRelation(unsafe_unique_array<idx_t> relations, idx_t count) {
	auto ret = make_uniq<JoinRelationSet>(relations, count);
	return GetJoinRelation(std::move(ret));
}

reference<JoinRelationSet> JoinRelationSetManager::GetJoinRelation(unique_ptr<JoinRelationSet> set) {
	auto existing = active_relation_sets.find(set->relations);
	if (existing == active_relation_sets.end()) {
		auto copy = make_uniq<JoinRelationSet>(set->Copy());
		active_relation_sets[set->relations] = std::move(set);
		set = std::move(copy);
	}
	auto ret = active_relation_sets.find(set->relations);
	auto &wat = *ret->second;
	return wat;
}

//! Create or get a JoinRelationSet from a single node with the given index
reference<JoinRelationSet> JoinRelationSetManager::GetJoinRelation(idx_t index) {
	// create a sorted vector of the relations
	auto relations = make_unsafe_uniq_array<idx_t>(1);
	relations[0] = index;
	idx_t count = 1;
	return GetJoinRelation(std::move(relations), count);
}

reference<JoinRelationSet> JoinRelationSetManager::GetJoinRelation(const unordered_set<idx_t> &bindings) {
	// create a sorted vector of the relations
	unsafe_unique_array<idx_t> relations = bindings.empty() ? nullptr : make_unsafe_uniq_array<idx_t>(bindings.size());
	idx_t count = 0;
	for (auto &entry : bindings) {
		relations[count++] = entry;
	}
	std::sort(relations.get(), relations.get() + count);
	return GetJoinRelation(std::move(relations), count);
}

reference<JoinRelationSet> JoinRelationSetManager::Union(JoinRelationSet &left, JoinRelationSet &right) {
	auto left_copy = make_uniq<JoinRelationSet>(left.Copy());
	auto right_copy = right.Copy();
	left_copy->relations |= right_copy.relations;
	return GetJoinRelation(std::move(left_copy));
}

// JoinRelationSet *JoinRelationSetManager::Difference(JoinRelationSet *left, JoinRelationSet *right) {
// 	auto relations = unsafe_unique_array<idx_t>(new idx_t[left->count]);
// 	idx_t count = 0;
// 	// move through the left and right relations
// 	idx_t i = 0, j = 0;
// 	while (true) {
// 		if (i == left->count) {
// 			// exhausted left relation, we are done
// 			break;
// 		} else if (j == right->count) {
// 			// exhausted right relation, add remaining of left
// 			for (; i < left->count; i++) {
// 				relations[count++] = left->relations[i];
// 			}
// 			break;
// 		} else if (left->relations[i] == right->relations[j]) {
// 			// equivalent, add nothing
// 			i++;
// 			j++;
// 		} else if (left->relations[i] < right->relations[j]) {
// 			// left is smaller, progress left and add it to the set
// 			relations[count++] = left->relations[i];
// 			i++;
// 		} else {
// 			// right is smaller, progress right
// 			j++;
// 		}
// 	}
// 	return GetJoinRelation(std::move(relations), count);
// }

// static string JoinRelationTreeNodeToString(const JoinRelationTreeNode *node) {
// 	string result = "";
// 	if (node->relation) {
// 		result += node->relation.get()->ToString() + "\n";
// 	}
// 	for (auto &child : node->children) {
// 		result += JoinRelationTreeNodeToString(child.second.get());
// 	}
// 	return result;
// }

// string JoinRelationSetManagerOld::ToString() const {
// 	return JoinRelationTreeNodeToString(&root);
// }
//
// void JoinRelationSetManagerOld::Print() {
// 	Printer::Print(ToString());
// }

} // namespace duckdb
