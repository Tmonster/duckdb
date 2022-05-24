//
// Created by Tom Ebergen on 5/16/22.
//

#include "duckdb/common/pair.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_node.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

static const double default_selectivity = 0.2;

void JoinNode::InitColumnStats(vector<FilterInfo *> filters, JoinOrderOptimizer *optimizer) {
	if (init_stats) {
		return;
	}
	join_stats->right_col_sel = 1;
	join_stats->right_col_mult = 1;
	join_stats->left_col_sel = 1;
	join_stats->left_col_mult = 1;

	JoinRelationSet *join_relations = set;
	idx_t relation_id;
	bool found_table_index = false;
	for (idx_t it = 0; it < join_relations->count; it++) {
		relation_id = join_relations->relations[it];

#ifdef DEBUG
		unordered_map<idx_t, idx_t>::iterator relation_map_it;
		found_table_index = false;
		for (relation_map_it = optimizer->relation_mapping.begin();
		     relation_map_it != optimizer->relation_mapping.end(); relation_map_it++) {
			if (relation_map_it->second == relation_id) {
				found_table_index = true;
				break;
			}
		}
		D_ASSERT(found_table_index);
#endif

		if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
			auto tmp = optimizer->relations.at(relation_id)->op;
			auto &get = (LogicalGet &)*tmp;
			auto catalog_table = get.GetTable();

			if (catalog_table) {
				// Get HLL stats here
//				auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, 0);
//				if (base_stats.distinct_stats)
			}
			if (!get.table_filters.filters.empty()) {
				has_filter = true;
			}
		}
		else if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (optimizer->relations.at(relation_id)->op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
				auto &get = (LogicalGet&)*optimizer->relations.at(relation_id)->op->children[0];
				auto catalog_table = get.GetTable();
				if (catalog_table) {
					// Get HLL stats here.
				}
			}
			has_filter = true;
		}
		calculate_unique_count(relation_id, optimizer, has_filter);
	}
	init_stats = true;
}

void JoinNode::calculate_unique_count(idx_t relation_id, JoinOrderOptimizer *optimizer, bool has_filter) {
	unordered_set<idx_t>::iterator ite;
	for (ite = optimizer->relation_to_columns[relation_id].begin();
	     ite != optimizer->relation_to_columns[relation_id].end(); ite++) {
		// insert the table, col pair into the resulting join node.
		join_stats->table_cols[relation_id].insert(*ite);
		auto index = hash_table_col(relation_id, *ite);

		// with HLL you can improve the unique values count.
		join_stats->table_col_unique_vals[index] = cardinality;
		if (has_filter) {
			// when adding HLL, first identify which column has the filter, then apply
			// selectivity in the following way
			// join_stats->table_col_unique_vals = min(count of filtered column, count of current column)
			join_stats->table_col_unique_vals[index] *= 0.2;
		}
	}
}

double JoinNode::GetTableColMult(idx_t table, idx_t col) {
	auto hash = hash_table_col(table, col);
	D_ASSERT(key_exists(hash, join_stats->table_col_unique_vals));
	D_ASSERT(join_stats->table_col_unique_vals[hash] >= 0);
	return cardinality / join_stats->table_col_unique_vals[hash];
}

void JoinNode::update_cardinality_estimate(bool same_base_table) {

	JoinNode *min_count_node = right;
	JoinNode *max_count_node = left;
	auto left_hash = hash_table_col(join_stats->base_table_left, join_stats->base_column_left);
	auto right_hash = hash_table_col(join_stats->base_table_right, join_stats->base_column_right);

	idx_t max_count_table = join_stats->base_table_left;
	idx_t max_count_column = join_stats->base_column_left;
	if (left->join_stats->table_col_unique_vals[left_hash] < right->join_stats->table_col_unique_vals[right_hash]) {
		min_count_node = left;
		max_count_node = right;
		max_count_table = join_stats->base_table_right;
		max_count_column = join_stats->base_column_right;
	}

	// Find the table with *more* unique values in the joined columns.
	// W.L.O.G suppose column a from a table A is joining with column a from table B.
	// and A[a].count < B[a].count
	// A has less unique values in column a. Assume all are matched in B[a].
	cardinality = max_count_node->GetTableColMult(max_count_table, max_count_column) * min_count_node->cardinality;
	join_stats->cardinality = cardinality;
}

void JoinNode::update_cost() {
	cost = cardinality + left->cost + right->cost;
	if (left->cost == 0) {
		cost += left->cardinality;
	}
	if (right->cost == 0) {
		cost += right->cardinality;
	}
	join_stats->cost = cost;
}

void JoinNode::update_stats_from_joined_tables(idx_t left_pair_key, idx_t right_pair_key) {

	idx_t cur_left_table, cur_right_table;
	unordered_set<idx_t>::iterator col_it;
	join_stats->table_col_unique_vals[left_pair_key] = MinValue(right->join_stats->table_col_unique_vals[right_pair_key],
	                                                            left->join_stats->table_col_unique_vals[left_pair_key]);
	join_stats->table_col_unique_vals[right_pair_key] = join_stats->table_col_unique_vals[left_pair_key];
	//! 5) update all other left multiplicities of columns in the joined table(s)
	for (idx_t table_it = 0; table_it < left->set->count; table_it++) {
		cur_left_table = left->set->relations[table_it];
		for (col_it = left->join_stats->table_cols[cur_left_table].begin();
		     col_it != left->join_stats->table_cols[cur_left_table].end(); col_it++) {

			join_stats->table_cols[cur_left_table].insert(*col_it);
			auto left_table_hash = hash_table_col(cur_left_table, *col_it);
			join_stats->table_col_unique_vals[left_table_hash] =
			    left->join_stats->table_col_unique_vals[left_table_hash];
		}
	}
	for (idx_t table_it = 0; table_it < right->set->count; table_it++) {
		cur_right_table = right->set->relations[table_it];
		for (col_it = right->join_stats->table_cols[cur_right_table].begin();
		     col_it != right->join_stats->table_cols[cur_right_table].end(); col_it++) {

			join_stats->table_cols[cur_right_table].insert(*col_it);
			auto right_table_hash = hash_table_col(cur_right_table, *col_it);
			join_stats->table_col_unique_vals[right_table_hash] =
			    right->join_stats->table_col_unique_vals[right_table_hash];
		}
	}
}

//! Check to make sure all columns have mult and sel values in the resulting Join Node
void JoinNode::check_all_table_keys_forwarded() {
	idx_t rights_column_count = 0;
	idx_t key;
	unordered_map<idx_t, unordered_set<idx_t>>::iterator tab_col_iterator;
	unordered_map<idx_t, double>::iterator val_iterator;
	for (tab_col_iterator = right->join_stats->table_cols.begin();
	     tab_col_iterator != right->join_stats->table_cols.end(); tab_col_iterator++) {
		rights_column_count += tab_col_iterator->second.size();
	}
	D_ASSERT(rights_column_count == right->join_stats->table_col_unique_vals.size());

	for (val_iterator = right->join_stats->table_col_unique_vals.begin();
	     val_iterator != right->join_stats->table_col_unique_vals.end(); val_iterator++) {
		key = val_iterator->first;
		D_ASSERT(key_exists(key, join_stats->table_col_unique_vals));
		D_ASSERT(join_stats->table_col_unique_vals[key] >= 1);
	}

	idx_t lefts_column_count = 0;
	for (tab_col_iterator = left->join_stats->table_cols.begin(); tab_col_iterator != left->join_stats->table_cols.end();
	     tab_col_iterator++) {
		lefts_column_count += tab_col_iterator->second.size();
	}
	D_ASSERT(lefts_column_count == left->join_stats->table_col_unique_vals.size());
	for (val_iterator = left->join_stats->table_col_unique_vals.begin();
	     val_iterator != left->join_stats->table_col_unique_vals.end(); val_iterator++) {
		key = val_iterator->first;
		D_ASSERT(key_exists(key, join_stats->table_col_unique_vals));
		D_ASSERT(join_stats->table_col_unique_vals[key] >= 1);
	}

	D_ASSERT(join_stats->table_col_unique_vals.size() ==
	         (right->join_stats->table_col_unique_vals.size() +
	          left->join_stats->table_col_unique_vals.size()));
}

bool JoinNode::key_exists(idx_t key, unordered_map<idx_t, double> stat_column) {
	return stat_column.find(key) != stat_column.end();
}

idx_t JoinNode::hash_table_col(idx_t table, idx_t col) {
	return (table << 32) + col;
}

//! ******************************************************
//! *          START OF DEBUGGING FUNCTIONS              *
//! ******************************************************
bool JoinNode::desired_relation_set(JoinRelationSet *relation_set, unordered_set<idx_t> o_set) {
	for (idx_t it = 0; it < relation_set->count; it++) {
		if (o_set.find(relation_set->relations[it]) == o_set.end()) {
			return false;
		}
	}
	return relation_set->count == o_set.size();
}

bool JoinNode::desired_join(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> desired_left,
                            unordered_set<idx_t> desired_right) {
	bool left_is_left = desired_relation_set(left, desired_left) && desired_relation_set(right, desired_right);
	bool right_is_left = desired_relation_set(right, desired_left) && desired_relation_set(left, desired_right);
	return left_is_left || right_is_left;
}

void JoinNode::printWholeNode(JoinNode *node) {
	if (!node)
		return;
	PrintNodeUniqueValueStats(node);
	printWholeNode(node->left);
	printWholeNode(node->right);
}

void JoinNode::PrintNodeUniqueValueStats(JoinNode *node) {
	if (!node)
		return;
	unordered_map<idx_t, unordered_set<idx_t>>::iterator it;
	unordered_set<idx_t>::iterator col_it;
	idx_t table;
	idx_t col;
	idx_t key;
	std::string relations = "";
	for (idx_t rel_it = 0; rel_it < node->set->count; rel_it++) {
		relations += std::to_string(node->set->relations[rel_it]) + ", ";
	}
	std::cout << "relations = [" << relations << "] " << std::endl;
	std::cout << "Expected cardinality = " << std::to_string(node->cardinality) << std::endl;
	std::cout << "cost = " << node->cost << std::endl;
	for (it = node->join_stats->table_cols.begin(); it != node->join_stats->table_cols.end(); it++) {
		table = it->first;
		for (col_it = it->second.begin(); col_it != it->second.end(); col_it++) {
			col = *col_it;
			key = hash_table_col(table, col);
			std::cout << "node table_col_unique_vals[" << table << "][" << col << "] = " << node->join_stats->table_col_unique_vals[key]
			          << std::endl;
		}
	}
	if (node->set->count > 1) {
		std::cout << "join on [" << node->join_stats->base_table_left << "][" << node->join_stats->base_column_left;
		std::cout << "] = [" << node->join_stats->base_table_right << "][" << node->join_stats->base_column_right << "]"
		          << std::endl;
	}
	std::cout << "----------------------------------" << std::endl;
}

} // namespace duckdb