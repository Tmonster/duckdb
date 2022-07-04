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
static const idx_t readable_offset = 10000000;



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


unique_ptr<TableFilterStats> JoinNode::InspectTableFilters(TableFilterSet *filters,
                                                           TableCatalogEntry *catalog_table,
                                                           JoinOrderOptimizer *optimizer) {
	unordered_map<idx_t, unique_ptr<TableFilter>>::iterator it;
	unique_ptr<TableFilterStats> stats = make_unique<TableFilterStats>();
	stats->has_equality_filter = false;
	for(it = filters->filters.begin(); it != filters->filters.end(); it++) {
		if (it->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			ConjunctionAndFilter &fil = (ConjunctionAndFilter &)*it->second;
			for (auto &child_filter : fil.child_filters) {
				if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
					auto comparison_filter = (ConstantFilter &)*child_filter;
					if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
						auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, it->first);
						auto is_us_filter = comparison_filter.constant.ToString().compare("[us]") == 0;
						auto col_count = base_stats->GetDistinctCount();
						if (stats->has_equality_filter) {
							stats->cardinality_with_equality_filter =
							    MinValue((idx_t)ceil(cardinality / col_count), stats->cardinality_with_equality_filter);
						} else {
							stats->cardinality_with_equality_filter = ceil(cardinality / col_count);
						}
//						if (is_us_filter) {
//							stats->cardinality_with_equality_filter = 84843;
//						}
						stats->has_equality_filter = true;
					}
				}
			}
		}
		else if (it->second->filter_type == TableFilterType::CONJUNCTION_OR) {
			ConjunctionOrFilter &fil = (ConjunctionOrFilter &)*it->second;
			for (auto &child_filter : fil.child_filters) {
				if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
					auto comparison_filter = (ConstantFilter &)*child_filter;
					if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
						auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, it->first);

						auto col_count = base_stats->GetDistinctCount();
						if (stats->has_equality_filter) {
							stats->cardinality_with_equality_filter += stats->cardinality_with_equality_filter;
						} else {
							stats->cardinality_with_equality_filter = ceil(cardinality / col_count);
						}
						stats->has_equality_filter = true;
					}
				}
			}
		}
	}
	return stats;
}

void JoinNode::InitTDoms(JoinOrderOptimizer *optimizer) {
	JoinRelationSet *join_relations = set;
	idx_t relation_id;
	TableCatalogEntry *catalog_table = NULL;

	D_ASSERT(join_relations->count == 1);
	unique_ptr<TableFilterStats> tablestats = NULL;
	relation_id = join_relations->relations[0];

	// First check table filters
	if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_GET) {
		auto tmp = optimizer->relations.at(relation_id)->op;
		auto &get = (LogicalGet &)*tmp;
		catalog_table = get.GetTable();
		if (!get.table_filters.filters.empty()) {
			has_filter = true;
			 tablestats = InspectTableFilters(&get.table_filters, catalog_table, optimizer);

		}
		optimizer->relation_to_table_name[relation_id] = catalog_table->name;
	} else if (optimizer->relations.at(relation_id)->op->type == LogicalOperatorType::LOGICAL_FILTER) {
		if (optimizer->relations.at(relation_id)->op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = (LogicalGet &)*optimizer->relations.at(relation_id)->op->children[0];
			catalog_table = get.GetTable();
			optimizer->relation_to_table_name[relation_id] = catalog_table->name;
			has_filter = true;
			if (!get.table_filters.filters.empty()) {
				if (!get.table_filters.filters.empty()) {
					has_filter = true;
					tablestats = InspectTableFilters(&get.table_filters, catalog_table, optimizer);
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

	//! estimate cardinality based off of filters.
	idx_t cardinality_with_filter = cardinality;
	idx_t direct_filter_hash = -1;
	if (has_filter) {
		// If the filter an equality filter?
		// if so, cardinality_with_filter = cardinality / tdom(column_with_filter).
		cardinality_with_filter = default_selectivity * cardinality;
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
		if (tablestats && tablestats->has_equality_filter) {
			cardinality_with_filter = tablestats->cardinality_with_equality_filter;
		}
	}


	//! Initialize the tdoms for all columns the relation uses in join conditions.
	unordered_set<idx_t>::iterator ite;
	idx_t count = 0;
	for (ite = optimizer->relation_to_columns[relation_id].begin();
	     ite != optimizer->relation_to_columns[relation_id].end(); ite++) {

		//! for every column in the relation, get the count via either HLL, or assume it to be
		//! the cardinality
		idx_t key = readable_hash(relation_id, *ite);
		if (catalog_table) {
			// Get HLL stats here
			auto table_col_hash = optimizer->relation_column_to_original_column[key];
			auto actual_column = JoinNode::GetColumnFromReadableHash(table_col_hash);

			auto base_stats = catalog_table->storage->GetStatistics(optimizer->context, actual_column);

			count = base_stats->GetDistinctCount();
			if (key == direct_filter_hash) {
				count = count * default_selectivity;
				if (count < 1) count = 1;
			}
			// HLL messed up, count can't be greater than cardinality
			if (count > cardinality) {
				count = cardinality;
			}
			if (base_stats->type == LogicalTypeId::INTEGER) {
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

idx_t JoinNode::GetTDom(idx_t table, idx_t column, JoinOrderOptimizer *optimizer) {
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

void JoinNode::UpdateCardinalityEstimate(JoinOrderOptimizer *optimizer) {

	idx_t tdom_of_join = GetTDom(join_stats->base_table_right, join_stats->base_column_right, optimizer);
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

void JoinNode::UpdateCost() {
	cost = cardinality + left->cost + right->cost;

	join_stats->cost = cost;
}

idx_t JoinNode::hash_table_col(idx_t table, idx_t col) {
	return (table << 32) + col;
}


idx_t JoinNode::readable_hash(idx_t table, idx_t col) {
	return table * readable_offset + col;
}

idx_t JoinNode::GetColumnFromReadableHash(idx_t hash) {
	return hash % readable_offset;
}

idx_t JoinNode::GetTableFromReadableHash(idx_t hash) {
	return idx_t ((int)hash / (int)readable_offset);
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
} // namespace duckdb