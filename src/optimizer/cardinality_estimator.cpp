//
// Created by Tom Ebergen on 7/6/22.
//
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

static TableCatalogEntry* GetCatalogTableEntry(LogicalOperator *op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto get = (LogicalGet *)op;
		TableCatalogEntry *entry = get->GetTable();
		return entry;
	}
	for (auto &child : op->children) {
		TableCatalogEntry *entry = GetCatalogTableEntry(child.get());
		if (entry != nullptr) {
			return entry;
		}
	}
	return nullptr;
}


bool CardinalityEstimator::SingleColumnFilter(FilterInfo *filter_info) {
	if (filter_info->left_set && filter_info->right_set) {
		// Both set
		return false;
	}
	//! Filter on one relation, (i.e string or range filter on a column).
	//! Grab the relation (should always be 1) and add it to the
	//! the equivalence_relations
	D_ASSERT(filter_info->set->count == 1);
	bool found = false;
	for (const column_binding_set_t& i_set : equivalent_relations) {
		if (i_set.count(filter_info->left_binding) > 0) {
			// found an equivalent filter
			return true;
		}
	}
	D_ASSERT(filter_info->set->relations[0] == filter_info->left_binding.table_index);
	auto key = ColumnBinding(filter_info->set->relations[0],
							 filter_info->left_binding.column_index);
	column_binding_set_t tmp;
	tmp.insert(key);
	equivalent_relations.push_back(tmp);
	return true;
}

vector<idx_t> CardinalityEstimator::DetermineMatchingEquivalentSets(FilterInfo *filter_info) {
	vector<idx_t> matching_equivalent_sets;
	auto equivalent_relation_index = 0;
	//! eri = equivalent relation index
	bool added_to_eri;

	vector<unordered_set<idx_t>>::iterator it;
	for (const column_binding_set_t& i_set : equivalent_relations) {
		added_to_eri = false;
		if (i_set.count(filter_info->left_binding) > 0) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
			added_to_eri = true;
		}
		//! don't add both left and right to the matching_equivalent_sets
		//! since both left and right get added to that index anyway.
		if (i_set.count(filter_info->right_binding) > 0 && !added_to_eri) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
		}
		equivalent_relation_index += 1;
	}
	return matching_equivalent_sets;
}


void CardinalityEstimator::AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets) {
	if (matching_equivalent_sets.size() > 1) {
		for (ColumnBinding i : equivalent_relations.at(matching_equivalent_sets[1])) {
			equivalent_relations.at(matching_equivalent_sets[0]).insert(i);
		}
		equivalent_relations.at(matching_equivalent_sets[1]).clear();
		// add all values of one set to the other, delete the empty one
	} else if (matching_equivalent_sets.size() == 1) {
		idx_t set_i = matching_equivalent_sets.at(0);
		equivalent_relations.at(set_i).insert(filter_info->left_binding);
		equivalent_relations.at(set_i).insert(filter_info->right_binding);
	} else if (matching_equivalent_sets.empty()) {
		column_binding_set_t tmp;
		tmp.insert(filter_info->left_binding);
		tmp.insert(filter_info->right_binding);
		equivalent_relations.push_back(tmp);
	}
}


void CardinalityEstimator::InitEquivalentRelations(vector<unique_ptr<FilterInfo>>* filter_infos) {
	//! For each filter, we fill keep track of the index of the equivalent relation set
	//! the left and right relation needs to be added to.
	vector<idx_t> matching_equivalent_sets;
	for (auto &filter: *filter_infos) {
		matching_equivalent_sets.clear();


		if (SingleColumnFilter(filter.get())) {
			continue;
		}
		//! assuming filters are only on one column in the left set
		//! and one column in the right set.
		D_ASSERT(filter->left_set->count == 1);
		D_ASSERT(filter->right_set->count == 1);

		matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
}

void CardinalityEstimator::InitTotalDomains() {
	vector<column_binding_set_t>::iterator it;
	//! erase empty equivalent relation sets
	for (it = equivalent_relations.begin(); it != equivalent_relations.end(); it++) {
		if (it->empty()) {
			equivalent_relations.erase(it);
		}
	}
	//! initialize equivalent relation tdom vector to have the same size as the
	//! equivalent relations.
	for (it = equivalent_relations.begin(); it != equivalent_relations.end(); it++) {
		equivalent_relations_tdom_hll.push_back(0);
		equivalent_relations_tdom_no_hll.push_back(NumericLimits<idx_t>::Maximum());
	}
}


idx_t CardinalityEstimator::GetTDom(ColumnBinding binding) {
	idx_t use_ind = 0;
	for (const column_binding_set_t& i_set : equivalent_relations) {
		if (i_set.count(binding) == 1) {
			auto tdom_hll = equivalent_relations_tdom_hll[use_ind];
			if (tdom_hll > 0) {
				return tdom_hll;
			}
			auto tdom_no_hll = equivalent_relations_tdom_no_hll[use_ind];
			if (tdom_no_hll > 0) {
				return tdom_no_hll;
			}
		}
		use_ind += 1;
	}
	throw NotImplementedException("Could not get Total domain of join. Most likely a bug in InitTdoms");
}

void CardinalityEstimator::EstimateCardinality(JoinNode *node) {
	idx_t tdom_join_right = GetTDom(node->right_binding);
#ifdef DEBUG
	idx_t tdom_join_left = GetTDom(node->left_binding);
	D_ASSERT(tdom_join_left == tdom_join_right);
#endif
	auto left_card = node->left->estimated_props->cardinality;
	auto right_card = node->right->estimated_props->cardinality;
	D_ASSERT(tdom_join_right != 0);
	D_ASSERT(tdom_join_right != NumericLimits<idx_t>::Maximum());
	node->cardinality = (left_card * right_card) / tdom_join_right;
	node->estimated_props->cardinality = node->cardinality;
}

static bool IsLogicalFilter(LogicalOperator *op) {
	return op->type == LogicalOperatorType::LOGICAL_FILTER;
}

static bool IsLogicalGet(LogicalOperator *op) {
	return op->type == LogicalOperatorType::LOGICAL_GET;
}

static LogicalGet* GetLogicalGet(LogicalOperator *op) {
	LogicalGet *get = nullptr;
	if (IsLogicalGet(op)) {
		get = (LogicalGet*)op;
	}
	else if (IsLogicalFilter(op)) {
		auto child = op->children.at(0).get();
		if (IsLogicalGet(child)) {
			get = (LogicalGet*)child;
		}
		// if there are mark joins below the filter relations, get the catalog table for
		// the underlying sequential scan.
		if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			LogicalComparisonJoin *join = (LogicalComparisonJoin*)child;
			if (join->join_type == JoinType::MARK) {
				child = join->children[0].get();
				return GetLogicalGet(child);
			}
		}
	}
	return get;
}



void CardinalityEstimator::UpdateTotalDomains(JoinNode *node, LogicalOperator *op, vector<unique_ptr<FilterInfo>> *filter_infos) {
	idx_t relation_id = node->set->relations[0];

	TableCatalogEntry *catalog_table = nullptr;
	auto get = GetLogicalGet(op);
	if (get) {
		catalog_table = GetCatalogTableEntry(get);
	}

	//! Initialize the tdoms for all columns the relation uses in join conditions.
	unordered_set<idx_t>::iterator ite;
	idx_t count = 0;

	bool direct_filter = false;
	for (ite = relation_to_columns[relation_id].begin();
	     ite != relation_to_columns[relation_id].end(); ite++) {

		//! for every column in the relation, get the count via either HLL, or assume it to be
		//! the cardinality
		ColumnBinding key = ColumnBinding(relation_id, *ite);

		//TODO: Go through table filters and find if there is a direct filter
		// on a join filter
		if (catalog_table) {
			relation_to_table_name[node->set->relations[0]] = catalog_table->name;
			// Get HLL stats here
			auto actual_binding = relation_column_to_original_column[key];

			auto base_stats = catalog_table->storage->GetStatistics(context, actual_binding.column_index);
			count = base_stats->GetDistinctCount();

			//! means you have a direct filter on a column. The count/total domain for the column
			//! should be decreased to match the predicted total domain matching the filter.
			//! We decrease the total domain for all columns in the equivalence set because filter pushdown
			//! will mean all columns are affected.
			if (direct_filter) {
				count = count * DEFAULT_SELECTIVITY;
				if (count < 1) {
					count = 1;
				}
			}

			// HLL messed up, count can't be greater than cardinality
			if (count > node->cardinality) {
				count = node->cardinality;
			}
			if (base_stats->type == LogicalTypeId::INTEGER) {
				auto &numeric_stats = (NumericStatistics &)*base_stats;
				auto max_value = numeric_stats.max.GetValue<idx_t>();
				if (count > max_value) {
					count = max_value;
				}
			}
		} else {
			// No HLL. So if we know there is a direct filter, reduce count to cardinality with filter
			// otherwise assume the total domain is still the cardinality
			if (direct_filter) {
				count = node->estimated_props->cardinality;
			} else {
				count = node->cardinality;
			}
		}

		idx_t ind = 0;

		for (const column_binding_set_t& i_set : equivalent_relations) {
			if (i_set.count(key) == 1) {
				if (catalog_table) {
					if (equivalent_relations_tdom_hll.at(ind) < count) {
						equivalent_relations_tdom_hll.at(ind) = count;
					}
					if (equivalent_relations_tdom_no_hll.at(ind) > count) {
						equivalent_relations_tdom_no_hll.at(ind) = count;
					}
				} else {
					if (equivalent_relations_tdom_no_hll.at(ind) > count) {
						equivalent_relations_tdom_no_hll.at(ind) = count;
					}
					// If we join a parquet/csv table with a catalog table that has HLL
					// then count = cardinality.
					// In this case, prefer the lower values.
					// equivalent_relations_tdom_hll is initialized to 0 for all equivalence relations
					// so check for 0 to make sure an ideal value exists.
					if (equivalent_relations_tdom_hll.at(ind) > count || equivalent_relations_tdom_hll.at(ind) == 0) {
						equivalent_relations_tdom_hll.at(ind) = count;
					}

				}
			}
			ind += 1;
		}
	}
}

TableFilterSet *CardinalityEstimator::GetTableFilters(LogicalOperator *op) {
	// First check table filters
	auto get = GetLogicalGet(op);
	if (get) {
		return &get->table_filters;
	}
	return nullptr;
}


idx_t CardinalityEstimator::InspectConjunctionAND( idx_t cardinality,
                                                  idx_t column_index,
                                                 ConjunctionAndFilter *filter,
                                                 TableCatalogEntry *catalog_table) {
	bool has_equality_filter = false;
	idx_t cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto comparison_filter = (ConstantFilter &)*child_filter;
			if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				auto base_stats = catalog_table->storage->GetStatistics(context, column_index);
				auto column_count = base_stats->GetDistinctCount();
				if (has_equality_filter) {
					cardinality_after_filters =
					    MinValue((idx_t)ceil(cardinality / column_count), cardinality_after_filters);
				} else {
					cardinality_after_filters = ceil(cardinality / column_count);
				}
				has_equality_filter = true;
			}
		}
	}
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectConjunctionOR(idx_t cardinality,
                                                 idx_t column_index,
                                                 ConjunctionOrFilter *filter,
                                                 TableCatalogEntry *catalog_table) {
	bool has_equality_filter = false;
	idx_t cardinality_after_filters = cardinality;
	for (auto &child_filter : filter->child_filters) {
		if (child_filter->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto comparison_filter = (ConstantFilter &)*child_filter;
			if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
				auto base_stats = catalog_table->storage->GetStatistics(context, column_index);
				auto column_count = base_stats->GetDistinctCount();
				auto increment = ceil(cardinality / column_count);
				if (has_equality_filter) {
					cardinality_after_filters += increment;
				} else {
					cardinality_after_filters = increment;
				}
				has_equality_filter = true;
			}
		}
	}
	return cardinality_after_filters;
}

idx_t CardinalityEstimator::InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters) {
	auto catalog_table = GetCatalogTableEntry(op);
	unordered_map<idx_t, unique_ptr<TableFilter>>::iterator it;
	idx_t cardinality_after_filters = cardinality;
	for (it = table_filters->filters.begin(); it != table_filters->filters.end(); it++) {
		if (it->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			ConjunctionAndFilter &filter = (ConjunctionAndFilter &)*it->second;
			idx_t cardinality_with_and_filter = InspectConjunctionAND(cardinality, it->first, &filter, catalog_table);
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
		} else if (it->second->filter_type == TableFilterType::CONJUNCTION_OR) {
			ConjunctionOrFilter &filter = (ConjunctionOrFilter &)*it->second;
			idx_t cardinality_with_or_filter = InspectConjunctionOR(cardinality, it->first, &filter, catalog_table);
			cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_or_filter);
		}
	}
	// if the above code didn't find an equality filter (i.e country_code = "[us]")
	// and there are other table filters, use default selectivity.
	bool has_equality_filter = (cardinality_after_filters != cardinality);
	if (!has_equality_filter && !table_filters->filters.empty()) {
		cardinality_after_filters = cardinality* DEFAULT_SELECTIVITY;
	}
	return cardinality_after_filters;
}

void CardinalityEstimator::EstimateBaseTableCardinality(JoinNode *node,
														LogicalOperator *op) {
	auto has_logical_filter = IsLogicalFilter(op);
	auto table_filters = GetTableFilters(op);
	// RunSampleFilter();

	auto estimated_filter_card = node->cardinality;
	// Logical Filter on a seq scan
	if (has_logical_filter) {
		estimated_filter_card = node->cardinality * DEFAULT_SELECTIVITY;
	} else if (table_filters) {
		estimated_filter_card = MinValue((double)InspectTableFilters(node->cardinality, op, table_filters), (double)estimated_filter_card);
	}
	node->estimated_props->cardinality = estimated_filter_card;
}


}