#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"

namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter, TableFilterType conjunction_type) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			if (conjunction_type == TableFilterType::CONJUNCTION_OR) {
				throw InternalException("Conjunction AND should not be connecting conjunction ors.");
			}
			auto &and_filter = entry->second->Cast<ConjunctionAndFilter>();
			and_filter.child_filters.push_back(std::move(filter));
		} else if (entry->second->filter_type == TableFilterType::CONJUNCTION_OR) {
			// Or filter connecting ands
			if (conjunction_type == TableFilterType::CONJUNCTION_AND) {
				auto and_filter = make_uniq<ConjunctionAndFilter>();
				and_filter->child_filters.push_back(std::move(entry->second));
				and_filter->child_filters.push_back(std::move(filter));
				filters[column_index] = std::move(and_filter);
				return;
			}
			auto &or_filter = entry->second->Cast<ConjunctionOrFilter>();
			or_filter.child_filters.push_back(std::move(filter));
		} else {
			if (conjunction_type == TableFilterType::CONJUNCTION_OR) {
				auto or_filter = make_uniq<ConjunctionOrFilter>();
				or_filter->child_filters.push_back(std::move(entry->second));
				or_filter->child_filters.push_back(std::move(filter));
				filters[column_index] = std::move(or_filter);
				return;
			}
			D_ASSERT(conjunction_type == TableFilterType::CONJUNCTION_AND);
			auto and_filter = make_uniq<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[column_index] = std::move(and_filter);
		}
	}
}

} // namespace duckdb
