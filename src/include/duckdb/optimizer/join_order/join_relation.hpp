//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct JoinRelationSet {
	JoinRelationSet() {
	}
	JoinRelationSet(unsafe_unique_array<idx_t> &relations_, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			relations[relations_[i]] = true;
		}
	}
	static void EnumerateRelations(std::bitset<12> relations, const std::function<void(idx_t relation)> &callback);

	string ToString() const;
	idx_t Count() const;
	idx_t NextNeighbor(idx_t i);
	std::bitset<12> relations;

	static bool IsSubset(JoinRelationSet &super, JoinRelationSet &sub);
	JoinRelationSet Copy() const;
};

//! The JoinRelationTree is a structure holding all the created JoinRelationSet objects and allowing fast lookup on to
//! them
class JoinRelationSetManager {

public:
	//! Create or get a JoinRelationSet from a single node with the given index
	reference<JoinRelationSet> GetJoinRelation(idx_t index);
	//! Create or get a JoinRelationSet from a set of relation bindings
	reference<JoinRelationSet> GetJoinRelation(const unordered_set<idx_t> &bindings);
	//! Create or get a JoinRelationSet from a (sorted, duplicate-free!) list of relations
	reference<JoinRelationSet> GetJoinRelation(unsafe_unique_array<idx_t> relations, idx_t count);
	//! Create or get a JoinRelationSet from another JoinRelation Set
	reference<JoinRelationSet> GetJoinRelation(unique_ptr<JoinRelationSet> set);
	//! Union two sets of relations together and create a new relation set
	reference<JoinRelationSet> Union(JoinRelationSet &left, JoinRelationSet &right);
	// //! Create the set difference of left \ right (i.e. all elements in left that are not in right)
	// JoinRelationSet *Difference(JoinRelationSet *left, JoinRelationSet *right);
	string ToString() const;
	void Print();

private:
	unordered_map<std::bitset<12>, unique_ptr<JoinRelationSet>> active_relation_sets;
};

} // namespace duckdb
