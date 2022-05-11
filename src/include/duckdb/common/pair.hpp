//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/pair.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

namespace duckdb {
using std::make_pair;
using std::pair;

} // namespace duckdb

template<>
struct std::hash<std::pair<duckdb::idx_t, duckdb::idx_t>> {
	inline size_t operator()(const pair<duckdb::idx_t, duckdb::idx_t>& the_pair) const {
		// size_t value = your hash computations over x
		return ((the_pair.first << 32) + the_pair.second);
	}
};