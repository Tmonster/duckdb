#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaTableSampleFunctionData : public TableFunctionData {
	explicit PragmaTableSampleFunctionData(CatalogEntry &entry_p) : entry(entry_p) {
	}

	CatalogEntry &entry;
};

struct PragmaTableOperatorData : public GlobalTableFunctionState {
	PragmaTableOperatorData() : offset(0) {
	}
	idx_t offset;
};

static unique_ptr<FunctionData> PragmaTableSampleBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// get the 

}

unique_ptr<GlobalTableFunctionState> PragmaTableSampleInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaTableOperatorData>();
}

static void PragmaTableSampleTable(PragmaTableOperatorData &data, TableCatalogEntry &table, DataChunk &output) {
	// if table has statistics.
	// copy the statistics into the output chunk.
}

static void PragmaTableSampleFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaTableSampleFunctionData>();
	auto &state = data_p.global_state->Cast<PragmaTableOperatorData>();
	switch (bind_data.entry.type) {
	case CatalogType::TABLE_ENTRY:
		PragmaTableSampleTable(state, bind_data.entry.Cast<TableCatalogEntry>(), output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_sample");
	}
}

void PragmaTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_table_sample", {LogicalType::VARCHAR}, PragmaTableSampleFunction,
	                              PragmaTableSampleBind, PragmaTableSampleInit));
}

} // namespace duckdb
