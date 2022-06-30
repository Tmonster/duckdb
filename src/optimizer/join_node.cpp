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
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

#include <algorithm>
#include <iostream>

namespace duckdb {

static const double default_selectivity = 0.2;


TableCatalogEntry* JoinNode::GetCatalogTableEntry(LogicalOperator *op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto get = (LogicalGet*)op;
		TableCatalogEntry *entry = get->GetTable();
		return entry;
	}
	for(auto &child: op->children) {
		TableCatalogEntry *entry = GetCatalogTableEntry(child.get());
		if (entry != NULL) return entry;
	}
	return NULL;
}



void JoinNode::InitTDoms(JoinOrderOptimizer *optimizer) {
	JoinRelationSet *join_relations = set;
	idx_t relation_id;
	TableCatalogEntry *catalog_table = NULL;
	bool has_equality_table_filter = false;
	idx_t cardinality_with_equality_table_filter = 0;
	D_ASSERT(join_relations->count == 1);
	relation_id = join_relations->relations[0];
	bool got_catalog = false;
	if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
		auto tmp = optimizer->relations.at(relation_id)->op;
		auto &get = (LogicalGet &)*tmp;
		catalog_table = get.GetTable();
		if (!get.table_filters.filters.empty()) {
			has_filter = true;
			unordered_map<idx_t, unique_ptr<TableFilter>>::iterator it;
			for(it = get.table_filters.filters.begin(); it != get.table_filters.filters.end(); it++) {
//				std::cout << std::to_string(it->second->filter_type == TableFilterType::CONJUNCTION_AND) << std::endl;

				ConjunctionAndFilter &fil = (ConjunctionAndFilter&)*it->second;
				for (auto &child_filter: fil.child_filters) {
					if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
						auto comparison_filter = (ConstantFilter&)*child_filter;
						if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
//							std::cout << "table has an equality filter" << std::endl;
//							std::cout << "the relation_id, idx_t is " << relation_id << ", " << it->first << std::endl;

							auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, it->first);

							auto col_count = base_stats->GetDistinctCount();
							has_equality_table_filter = true;
							cardinality_with_equality_table_filter = ceil(cardinality / col_count);
//							std::cout << "direct filter on column in " << catalog_table->name << ". Count = " << col_count << std::endl;
						}
					}
				}

			}
		}
		optimizer->relation_to_table_name[relation_id] = catalog_table->name;
	} else if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_FILTER) {
		if (optimizer->relations.at(relation_id)->op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = (LogicalGet &)*optimizer->relations.at(relation_id)->op->children[0];
			catalog_table = get.GetTable();
			optimizer->relation_to_table_name[relation_id] = catalog_table->name;
			has_filter = true;
			if (!get.table_filters.filters.empty()) {
				unordered_map<idx_t, unique_ptr<TableFilter>>::iterator it;
				for(it = get.table_filters.filters.begin(); it != get.table_filters.filters.end(); it++) {
//					std::cout << std::to_string(it->second->filter_type == TableFilterType::CONJUNCTION_AND) << std::endl;

					ConjunctionAndFilter &fil = (ConjunctionAndFilter&)*it->second;
					for (auto &child_filter: fil.child_filters) {
						if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
							auto comparison_filter = (ConstantFilter&)*child_filter;
							if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
								//							std::cout << "table has an equality filter" << std::endl;
								//							std::cout << "the relation_id, idx_t is " << relation_id << ", " << it->first << std::endl;

								auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, it->first);

								auto col_count = base_stats->GetDistinctCount();
								has_equality_table_filter = true;
								cardinality_with_equality_table_filter = ceil(cardinality / col_count);
								//							std::cout << "direct filter on column in " << catalog_table->name << ". Count = " << col_count << std::endl;
							}
						}
					}

				}
			}
		} else {
			// most likely dealing with a filter on top of a join
			// queries like id IN ('1', '2', '3') trigger this
			auto tmp = optimizer->relations.at(relation_id)->op;
			catalog_table = GetCatalogTableEntry(tmp);
			if (catalog_table) optimizer->relation_to_table_name[relation_id] = catalog_table->name;
			has_filter = true;
		}
	}


	idx_t cardinality_with_filter = cardinality;
	idx_t direct_filter_hash = -1;
	if (has_filter) {
		// If the filter an equality filter?
		// if so, cardinality_with_filter = cardinality / tdom(column_with_filter).
		cardinality_with_filter = 0.2 * cardinality;
		if (cardinality_with_filter < 1) cardinality_with_filter = 1;
		// check if the filter is on the column, how?
		for (auto &filter : optimizer->filter_infos) {
			// if there is a filter info where
			if (filter->set->count == 1 && filter->set->relations[0] == relation_id) {
				// then the filter is on this relation_id
				// the readable hash represents the table_column that has a direct filter.
				// that Tdom can be multiplied by 0.2.
				direct_filter_hash = readable_hash(relation_id, filter->left_binding.second);
			}
		}
		if (has_equality_table_filter) {
			cardinality_with_filter = cardinality_with_equality_table_filter;
		}
	}

	unordered_set<idx_t>::iterator ite;
	idx_t count = 0;
	for (ite = optimizer->relation_to_columns[relation_id].begin();
	     ite != optimizer->relation_to_columns[relation_id].end(); ite++) {

		//! for every column in the relation, get the count via either HLL, or assume it to be
		//! the cardinality
		idx_t key = readable_hash(relation_id, *ite);
//		if (key == JoinNode::readable_hash(1,0) || key == JoinNode::readable_hash(3,2)) {
//			std::cout << "break here" << std::endl;
//		}
		if (catalog_table) {
			// Get HLL stats here
			idx_t actual_column = optimizer->relation_column_to_original_column[key];
			auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, actual_column);

			count = base_stats->GetDistinctCount();
			if (key == direct_filter_hash) {
//				std::cout << "direct filter found" << std::endl;
				count = count * 0.2;
				if (count < 1) count = 1;
			}
			// HLL messed up, count can't be greater than cardinality
			if (count > cardinality) {
				count = cardinality;
			}
			if (base_stats->type == LogicalTypeId::INTEGER) {
				//TODO: check column type as well. Maybe wrap this in a try catch
				auto &numeric_stats = (NumericStatistics&)*base_stats;
				auto max_value = numeric_stats.max.GetValue<idx_t>();
				if (count > max_value) {
					count = max_value;
				}
			}
		} else {
			// No HLL. So if we know there is a direct filter, reduce count to cardinality with filter
			// otherwise assume the total domain is still the cardinality
			if (key == direct_filter_hash) {
//				std::cout << "direct filter found" << std::endl;
				count = cardinality_with_filter;
			} else {
				count = cardinality;
			}
		}

		idx_t ind = 0;

		for(unordered_set<idx_t> i_set : optimizer->equivalent_relations) {
			if (i_set.count(key) == 1) {
				if (catalog_table) {
					if (optimizer->equivalent_relations_tdom_hll.at(ind) < count) {
						optimizer->equivalent_relations_tdom_hll.at(ind) = count;
					}
					if (optimizer->equivalent_relations_tdom_no_hll.at(ind) > count) {
						optimizer->equivalent_relations_tdom_no_hll.at(ind) = count;
					}
				} else {
					if (optimizer->equivalent_relations_tdom_no_hll.at(ind) > count) {
						optimizer->equivalent_relations_tdom_no_hll.at(ind) = count;
					}
					optimizer->equivalent_relations_tdom_hll.at(ind) = count;
				}
			}
			ind+=1;
		}
	}
	join_stats->cardinality = cardinality_with_filter;
}

void JoinNode::InitColumnStats(JoinOrderOptimizer *optimizer) {
	return;
//	if (init_stats) {
//		return;
//	}
//	join_stats->right_col_sel = 1;
//	join_stats->right_col_mult = 1;
//	join_stats->left_col_sel = 1;
//	join_stats->left_col_mult = 1;
//
//	JoinRelationSet *join_relations = set;
//	idx_t relation_id;
//	TableCatalogEntry *catalog_table = NULL;
//
//	D_ASSERT(join_relations->count == 1);
//	for (idx_t it = 0; it < join_relations->count; it++) {
//		relation_id = join_relations->relations[it];
//
//#ifdef DEBUG
//		bool found_table_index = false;
//		unordered_map<idx_t, idx_t>::iterator relation_map_it;
//		found_table_index = false;
//		for (relation_map_it = optimizer->relation_mapping.begin();
//		     relation_map_it != optimizer->relation_mapping.end(); relation_map_it++) {
//			if (relation_map_it->second == relation_id) {
//				found_table_index = true;
//				break;
//			}
//		}
//		D_ASSERT(found_table_index);
//#endif
//
//		if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
//			auto tmp = optimizer->relations.at(relation_id)->op;
//			auto &get = (LogicalGet &)*tmp;
//			catalog_table = get.GetTable();
//			if (!get.table_filters.filters.empty()) {
//				has_filter = true;
//			}
//		} else if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_FILTER) {
//			if (optimizer->relations.at(relation_id)->op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
//				auto &get = (LogicalGet &)*optimizer->relations.at(relation_id)->op->children[0];
//				catalog_table = get.GetTable();
//				has_filter = true;
//			}
//		}
//	}
//
//	idx_t cardinality_with_filter = cardinality;
//	if (has_filter) {
//		// believe it or not, sometimes we predict a cardinality of 0
//		cardinality_with_filter = MaxValue(cardinality_with_filter * 0.2, (double)1);
//		join_stats->cardinality = cardinality_with_filter;
//	}
//
//	unordered_set<idx_t>::iterator ite;
//	idx_t index;
//	idx_t count = 0;
//	for (ite = optimizer->relation_to_columns[relation_id].begin();
//		 ite != optimizer->relation_to_columns[relation_id].end(); ite++) {
//		if (catalog_table) {
//			// Get HLL stats here
//			idx_t key = hash_table_col(relation_id, *ite);
//			idx_t actual_column = optimizer->relation_column_to_original_column[key];
//			auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, actual_column);
//			count = base_stats->GetDistinctCount();
////			std::cout << "relation " << relation_id << " count is = " << count << ". cardinality = " << cardinality << std::endl;
//			if (count > cardinality) {
//				count = cardinality;
//			}
//			join_stats->table_name_to_relation[catalog_table->name] = relation_id;
//
//			index = readable_hash(relation_id, *ite);
//			std::string col_name = catalog_table->columns.at(actual_column).Name();
//			join_stats->relation_column_to_column_name[index] = col_name;
//		}
//
//		index = hash_table_col(relation_id, *ite);
//		join_stats->table_cols[relation_id].insert(*ite);
//
//	}
//
//	init_stats = true;
}

double JoinNode::GetTableColMult(idx_t table, idx_t col) {
//	auto hash = hash_table_col(table, col);
//	D_ASSERT(key_exists(hash, join_stats->table_col_unique_vals));
//	D_ASSERT(join_stats->table_col_unique_vals[hash] >= 0);
//	// use ceil because we want to try and over estimate cardinalities
//	// most optimizers still underestimate
//	return (double)cardinality / join_stats->table_col_unique_vals[hash];
	return (double)1;
}

idx_t JoinNode::getTdom(idx_t table, idx_t column, JoinOrderOptimizer *optimizer) {
	idx_t use_ind = 0;
	idx_t key = readable_hash(table, column);
	for(unordered_set<idx_t> i_set : optimizer->equivalent_relations) {
		if (i_set.count(key) == 1) {
			return optimizer->equivalent_relations_tdom_hll[use_ind];
		}
		use_ind+=1;
	}
	throw NotImplementedException("shouldn't get here actually");
}

void JoinNode::update_cardinality_estimate(JoinOrderOptimizer *optimizer) {

	idx_t tdom_of_join = getTdom(join_stats->base_table_right, join_stats->base_column_right, optimizer);
	auto left_card = left->cardinality;
	auto right_card = right->cardinality;
	if (left->has_filter) {
		left_card = left->join_stats->cardinality;
	}
	if (right->has_filter) {
		right_card = right->join_stats->cardinality;
	}
	cardinality = (left_card * right_card)/tdom_of_join;
	join_stats->cardinality = cardinality;
}

void JoinNode::update_cost() {
	cost = cardinality + left->cost + right->cost;

	//! when joining base tables, be sure to consider the cost of scanning the base tables.
	if (left->cost == 0) {
		cost += left->cardinality;
	}
	if (right->cost == 0) {
		cost += right->cardinality;
	}

	join_stats->cost = cost;
}

void JoinNode::update_stats_from_joined_tables(idx_t left_table, idx_t left_column, idx_t right_table, idx_t right_column) {

	idx_t left_pair_key = hash_table_col(left_table, left_column);
	idx_t right_pair_key = hash_table_col(right_table, right_column);
	idx_t cur_left_table, cur_right_table;
	unordered_set<idx_t>::iterator col_it;

	D_ASSERT(cardinality > 0);
	//! 5) update all other left multiplicities of columns in the joined table(s)
	for (idx_t table_it = 0; table_it < left->set->count; table_it++) {
		cur_left_table = left->set->relations[table_it];
		for (col_it = left->join_stats->table_cols[cur_left_table].begin();
		     col_it != left->join_stats->table_cols[cur_left_table].end(); col_it++) {

			join_stats->table_cols[cur_left_table].insert(*col_it);
			auto left_table_hash = hash_table_col(cur_left_table, *col_it);

			join_stats->table_col_unique_vals[left_table_hash] =
			    left->join_stats->table_col_unique_vals[left_table_hash];

			auto table_col_hash = readable_hash(cur_left_table, *col_it);
			join_stats->relation_column_to_column_name[table_col_hash] = left->join_stats->relation_column_to_column_name[table_col_hash];
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

			auto table_col_hash = readable_hash(cur_left_table, *col_it);
			join_stats->relation_column_to_column_name[table_col_hash] =
			    right->join_stats->relation_column_to_column_name[table_col_hash];
		}
	}

	auto domain_left = left->join_stats->table_col_unique_vals[left_pair_key];
	auto domain_right = right->join_stats->table_col_unique_vals[right_pair_key];
	double lower_domain = MinValue(domain_left, domain_right);
	join_stats->table_col_unique_vals[left_pair_key] = lower_domain;
	join_stats->table_col_unique_vals[right_pair_key] = lower_domain;
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

idx_t JoinNode::readable_hash(idx_t table, idx_t col) {
	return table * 10000000 + col;
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