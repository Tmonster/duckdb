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
#include <limits>

namespace duckdb {

static const double default_selectivity = 0.2;

void JoinNode::InitColumnStats(vector<FilterInfo *> filters, JoinOrderOptimizer *optimizer) {
	if (init_stats) {
		return;
	}
	join_stats.right_col_sel = 1;
	join_stats.right_col_mult = 1;
	join_stats.left_col_sel = 1;
	join_stats.left_col_mult = 1;

	JoinRelationSet *join_relations = set;
	idx_t relation_id;
	bool has_filter = false;
	for (idx_t it = 0; it < join_relations->count; it++) {
		relation_id = join_relations->relations[it];

		unordered_map<idx_t, idx_t>::iterator relation_map_it;
		//		idx_t table_index;
		bool found_table_index = false;
		for (relation_map_it = optimizer->relation_mapping.begin(); relation_map_it != optimizer->relation_mapping.end(); relation_map_it++) {
			if (relation_map_it->second == relation_id) {
				//	table_index = relation_map_it->first;
				found_table_index = true;
			}
		}
		D_ASSERT(found_table_index);

		if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
			auto tmp = optimizer->relations.at(relation_id)->op;
			auto &get = (LogicalGet &)*tmp;
			auto catalog_table = get.GetTable();
			if (catalog_table) {
				optimizer->relation_to_table_name[relation_id] = catalog_table->name;
				// can use catalog_table to get table stats and also update the mult for each column
				// we might be querying directly on files, so no catalog entry in that case
				std::vector<const char*> look_for_me = {"r_regionkey", "o_orderdate", "r_name", "n_regionkey", "n_nationkey", "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"};
				unordered_set<idx_t>::iterator rc_it;
				std::vector<duckdb::ColumnDefinition>::iterator co_it;
				int col_num;
	//			for(rc_it = relation_to_columns[relation_id].begin(); rc_it != relation_to_columns[relation_id].end(); rc_it++) {
	//				col_num = 0;
	//				for (co_it = catalog_table->columns.begin(); co_it != catalog_table->columns.end(); co_it++) {
	//					for (idx_t m = 0; m < 5; m++) {
	//						const char *watever = reinterpret_cast<const char *>(&(co_it->name));
	//						if (std::strcmp(look_for_me[m], watever) == 0) {
	////							if (!get.table_filters.filters.empty()) {
	////								std::cout << "column " << co_it->name << " has a filter"<< std::endl;
	////							}
	//							idx_t tmp_key = (relation_id << 32) + col_num;
	////							key_spies.insert(tmp_key);
	//						}
	//						col_num += 1;
	//					}
	//				}
	//			}
			}
			if (!get.table_filters.filters.empty()) {
				has_filter = true;
			}
		}

		vector<FilterInfo *>::iterator filter_it;
		idx_t right_table, right_column, left_table, left_column;
		for (filter_it = filters.begin(); filter_it != filters.end(); filter_it++) {
			right_table = (*filter_it)->right_binding.first;
			right_column = (*filter_it)->right_binding.second;
			left_table = (*filter_it)->left_binding.first;
			left_column = (*filter_it)->left_binding.second;
		}
		unordered_set<idx_t>::iterator ite;
		for (ite = optimizer->relation_to_columns[relation_id].begin(); ite != optimizer->relation_to_columns[relation_id].end(); ite++) {
			// this is where hyperloglog stats can be inserted
			join_stats.table_cols[relation_id].insert(*ite);
			auto index = hash_table_col(relation_id, *ite);
			join_stats.table_col_mults[index] = 1;
			if (has_filter && (
			                      (right_table == relation_id && *ite == right_column) ||
			                      (left_table == relation_id && *ite == left_column))) {
				join_stats.table_col_sels[index] = default_selectivity;
			}
			else join_stats.table_col_sels[index] = 1;
		}
	}
	init_stats = true;
}


void JoinNode::update_cardinality_estimate(bool same_base_table) {
	if (same_base_table) {
		// the base tables are the same, assume cross product cardinality.
		cardinality = (left->cardinality * right->cardinality);
	} else {
		D_ASSERT(join_stats.right_col_sel > 0 && join_stats.right_col_sel <= 1);
		D_ASSERT(join_stats.right_col_mult >= 1);
		cardinality = left->cardinality *
		              join_stats.left_col_sel *
		              join_stats.right_col_sel *
		              join_stats.right_col_mult;
	}
}


void JoinNode::update_cost() {
	cost = cardinality + left->cost + right->cost;
}

void JoinNode::update_cardinality_ratio(bool same_base_table) {
	if (right->cardinality == 0) {
		join_stats.cardinality_ratio = 1;
	} else if (same_base_table) {
		join_stats.cardinality_ratio = right->cardinality;
	} else {
		join_stats.cardinality_ratio = (double)left->cardinality / (double)right->cardinality;
	}
}

void JoinNode::update_stats_from_left_table(idx_t left_pair_key, idx_t right_pair_key) {
	//! 4) update the left multiplicities of the column in the equi-join
	//! result->table_col_mults[right_pair_key] is updated in step 1
	join_stats.table_col_mults[left_pair_key] = left->join_stats.table_col_mults[left_pair_key] *
	                                            MaxValue(right->join_stats.table_col_mults[right_pair_key], join_stats.cardinality_ratio);
//	std::cout << left->join_stats.table_col_mults[left_pair_key] << std::endl;
//	std::cout << join_stats.table_col_mults[right_pair_key] << std::endl;
	D_ASSERT(join_stats.table_col_mults[left_pair_key] >= 1);

	idx_t cur_left_table;
	unordered_set<idx_t>::iterator col_it;
	//! 5) update all other left multiplicities of columns in the joined table(s)
	for(idx_t table_it = 0; table_it < left->set->count; table_it++) {
		cur_left_table = left->set->relations[table_it];
		for (col_it = left->join_stats.table_cols[cur_left_table].begin(); col_it != left->join_stats.table_cols[cur_left_table].end(); col_it++) {

			join_stats.table_cols[cur_left_table].insert(*col_it);

			auto tmp_left_pair_key = (cur_left_table << 32) + *col_it;
			D_ASSERT(left->join_stats.table_col_mults.find(tmp_left_pair_key) != left->join_stats.table_col_mults.end());
			D_ASSERT(left->join_stats.table_col_sels.find(tmp_left_pair_key) != left->join_stats.table_col_sels.end());
			D_ASSERT(left->join_stats.table_col_sels.find(tmp_left_pair_key)->second <= 1);

			join_stats.table_col_sels[tmp_left_pair_key] = left->join_stats.table_col_sels[tmp_left_pair_key];

			D_ASSERT(join_stats.table_col_sels[tmp_left_pair_key] > 0 && join_stats.table_col_sels[tmp_left_pair_key] <= 1);

			//! already set the mult in step 4
			if (tmp_left_pair_key == left_pair_key) continue;
			join_stats.table_col_mults[tmp_left_pair_key] = left->join_stats.table_col_mults[tmp_left_pair_key];
			D_ASSERT(join_stats.table_col_mults[tmp_left_pair_key] >= 1);
//			if (cur_left_table == left_table) {
//				(*result->table_col_mults)[tmp_left_pair_key] =
//				    (*left->table_col_mults)[tmp_left_pair_key] * right_mult;
//			} else {
//				(*result->table_col_mults)[tmp_left_pair_key] =
//				    (*left->table_col_mults)[tmp_left_pair_key];
//			}
		}
	}
	join_stats.table_col_sels[left_pair_key] = right->join_stats.table_col_sels[right_pair_key];
}

void JoinNode::update_stats_from_right_table(idx_t left_pair_key, idx_t right_pair_key) {
	// iterate over a tables columns
	unordered_set<idx_t>::iterator col_it;
	idx_t cur_right_table;
	//! 3) update the rest of the right relations
	for(idx_t table_it = 0; table_it < right->set->count; table_it++) {
		cur_right_table = right->set->relations[table_it];
		//! loop to get all future joined columns that are joined under some condition
		for (col_it = right->join_stats.table_cols[cur_right_table].begin(); col_it != right->join_stats.table_cols[cur_right_table].end(); col_it++) {
			// always insert the table column entry
			// update selectivities too
			join_stats.table_cols[cur_right_table].insert(*col_it);

			auto tmp_right_pair_key = hash_table_col(cur_right_table, *col_it);
			D_ASSERT(right->join_stats.table_col_mults.find(tmp_right_pair_key) != right->join_stats.table_col_mults.end());
			D_ASSERT(right->join_stats.table_col_sels.find(tmp_right_pair_key) != right->join_stats.table_col_sels.end());
			join_stats.table_col_sels[tmp_right_pair_key] = right->join_stats.table_col_sels[tmp_right_pair_key];

			join_stats.table_col_mults[tmp_right_pair_key] =
			    right->join_stats.table_col_mults[tmp_right_pair_key] * MaxValue(join_stats.cardinality_ratio, left->join_stats.table_col_mults[left_pair_key]);
			D_ASSERT(join_stats.table_col_mults[tmp_right_pair_key] >= 1);
		}
	}
	double one = 1;

	//! update result mult for the column in the RESULT table originating from the RIGHT table
	if (left->join_stats.table_col_mults[left_pair_key] == 1) {
		join_stats.table_col_mults[right_pair_key] =
			MaxValue(one, right->join_stats.table_col_mults[right_pair_key] * join_stats.cardinality_ratio);
	} else {
		join_stats.table_col_mults[right_pair_key] =
			MaxValue(one, right->join_stats.table_col_mults[right_pair_key] * left->join_stats.table_col_mults[left_pair_key]);
	}
	D_ASSERT(join_stats.table_col_mults[right_pair_key] >= 1);

}

void JoinNode::check_all_table_keys_forwarded() {
	idx_t rights_column_count = 0;
	idx_t key;
	unordered_map<idx_t, unordered_set<idx_t>>::iterator tab_col_iterator;
	unordered_map<idx_t, double>::iterator val_iterator;
	for(tab_col_iterator = right->join_stats.table_cols.begin(); tab_col_iterator != right->join_stats.table_cols.end(); tab_col_iterator++) {
		rights_column_count += tab_col_iterator->second.size();
	}
	D_ASSERT(rights_column_count == right->join_stats.table_col_mults.size());
	D_ASSERT(rights_column_count == right->join_stats.table_col_sels.size());

	for(val_iterator = right->join_stats.table_col_mults.begin(); val_iterator != right->join_stats.table_col_mults.end(); val_iterator++) {
		key = val_iterator->first;
		D_ASSERT(key_exists(key, right->join_stats.table_col_mults));
		D_ASSERT(join_stats.table_col_mults[key] >= 1);
		D_ASSERT(key_exists(key, right->join_stats.table_col_sels));
		D_ASSERT(join_stats.table_col_sels[key] <= 1);
	}



	idx_t lefts_column_count = 0;
	for(tab_col_iterator = left->join_stats.table_cols.begin(); tab_col_iterator != left->join_stats.table_cols.end(); tab_col_iterator++) {
		lefts_column_count += tab_col_iterator->second.size();
	}
	D_ASSERT(lefts_column_count == left->join_stats.table_col_mults.size());
	D_ASSERT(lefts_column_count == left->join_stats.table_col_sels.size());
	for(val_iterator = left->join_stats.table_col_mults.begin(); val_iterator != left->join_stats.table_col_mults.end(); val_iterator++) {
		key = val_iterator->first;
		D_ASSERT(key_exists(key, left->join_stats.table_col_mults));
		D_ASSERT(join_stats.table_col_mults[key] >= 1);
		D_ASSERT(key_exists(key, left->join_stats.table_col_sels));
		D_ASSERT(join_stats.table_col_sels[key] <= 1);
	}

	D_ASSERT(join_stats.table_col_mults.size() == (right->join_stats.table_col_mults.size() + left->join_stats.table_col_mults.size()));
	D_ASSERT(join_stats.table_col_sels.size() == (right->join_stats.table_col_sels.size() + left->join_stats.table_col_sels.size()));
}


bool JoinNode::key_exists(idx_t key, unordered_map<idx_t, double> stat_column) {
	return stat_column.find(key) != stat_column.end();
}

idx_t JoinNode::hash_table_col(idx_t table, idx_t col) {
	return (table << 32) + col;
}

//! START OF DEBUGGING FUNCTIONS
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
	if (!node) return;
	PrintNodeSelMulStats(node);
	printWholeNode(node->left);
	printWholeNode(node->right);
}

void JoinNode::PrintNodeSelMulStats(JoinNode *node) {
	if (!node) return;
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
	for (it = node->join_stats.table_cols.begin(); it != node->join_stats.table_cols.end(); it++) {
		table = it->first;
		for (col_it = it->second.begin(); col_it != it->second.end(); col_it++) {
			col = *col_it;
			key = hash_table_col(table, col);
			std::cout << "node table_col_sel[" << table << "][" << col << "] = " << node->join_stats.table_col_sels[key] << std::endl;
			std::cout << "node table_col_mult[" << table << "][" << col << "] = " << node->join_stats.table_col_mults[key] << std::endl;
		}
	}
	std::cout << "node right_col_sel = " << node->join_stats.right_col_sel << std::endl;
	std::cout << "node right_col_mult = " << node->join_stats.right_col_mult << std::endl;
	std::cout << "node left_col_sel = " << node->join_stats.left_col_sel << std::endl;
	std::cout << "node left_col_mult = " << node->join_stats.left_col_mult << std::endl;
	if (node->set->count > 1) {
		std::cout << "join on [" << node->join_stats.base_table_left << "][" << node->join_stats.base_column_left;
		std::cout << "] = [" << node->join_stats.base_table_right << "][" << node->join_stats.base_column_right << "]" << std::endl;
	}
	std::cout << "----------------------------------" << std::endl;
}

}