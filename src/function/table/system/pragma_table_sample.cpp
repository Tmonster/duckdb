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

static unique_ptr<FunctionData> PragmaTableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {

}

unique_ptr<GlobalTableFunctionState> PragmaTableInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaTableOperatorData>();
}
//
//static void CheckConstraints(TableCatalogEntry &table, const ColumnDefinition &column, bool &out_not_null,
//                             bool &out_pk) {
//	out_not_null = false;
//	out_pk = false;
//	// check all constraints
//	// FIXME: this is pretty inefficient, it probably doesn't matter
//	for (auto &constraint : table.GetConstraints()) {
//		switch (constraint->type) {
//		case ConstraintType::NOT_NULL: {
//			auto &not_null = constraint->Cast<NotNullConstraint>();
//			if (not_null.index == column.Logical()) {
//				out_not_null = true;
//			}
//			break;
//		}
//		case ConstraintType::UNIQUE: {
//			auto &unique = constraint->Cast<UniqueConstraint>();
//			if (unique.is_primary_key) {
//				if (unique.index == column.Logical()) {
//					out_pk = true;
//				}
//				if (std::find(unique.columns.begin(), unique.columns.end(), column.GetName()) != unique.columns.end()) {
//					out_pk = true;
//				}
//			}
//			break;
//		}
//		default:
//			break;
//		}
//	}
//}

//static Value DefaultValue(const ColumnDefinition &def) {
//	if (def.Generated()) {
//		return Value(def.GeneratedExpression().ToString());
//	}
//	if (!def.HasDefaultValue()) {
//		return Value();
//	}
//	auto &value = def.DefaultValue();
//	return Value(value.ToString());
//}

static void PragmaTableInfoTable(PragmaTableOperatorData &data, TableCatalogEntry &table, DataChunk &output) {
	// if table has statistics.
	// copy the statistics into the output chunk.
}

static void PragmaTableInfoView(PragmaTableOperatorData &data, ViewCatalogEntry &view, DataChunk &output) {
	if (data.offset >= view.types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = MinValue<idx_t>(data.offset + STANDARD_VECTOR_SIZE, view.types.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto type = view.types[i];
		auto &name = view.aliases[i];
		// return values:
		// "cid", PhysicalType::INT32

		output.SetValue(0, index, Value::INTEGER((int32_t)i));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(name));
		// "type", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(type.ToString()));
		// "notnull", PhysicalType::BOOL
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", PhysicalType::VARCHAR
		output.SetValue(4, index, Value());
		// "pk", PhysicalType::BOOL
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

static void PragmaTableInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaTableSampleFunctionData>();
	auto &state = data_p.global_state->Cast<PragmaTableOperatorData>();
	switch (bind_data.entry.type) {
	case CatalogType::TABLE_ENTRY:
		PragmaTableInfoTable(state, bind_data.entry.Cast<TableCatalogEntry>(), output);
		break;
	case CatalogType::VIEW_ENTRY:
		PragmaTableInfoView(state, bind_data.entry.Cast<ViewCatalogEntry>(), output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_info");
	}
}

void PragmaTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_table_info", {LogicalType::VARCHAR}, PragmaTableInfoFunction,
	                              PragmaTableInfoBind, PragmaTableInfoInit));
}

} // namespace duckdb
