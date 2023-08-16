//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/columnref_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Represents a reference to a column from either the FROM clause or from an
//! alias
class ColumnRefExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::COLUMN_REF;

public:
	//! Specify both the column and table name
	ColumnRefExpression(string column_name, string table_name, bool from_sql = true);
	//! Only specify the column name, the table name will be derived later
	explicit ColumnRefExpression(string column_name, bool from_sql = true);
	//! Specify a set of names
	explicit ColumnRefExpression(vector<string> column_names, bool from_sql = true);

	//! The stack of names in order of which they appear (column_names[0].column_names[1].column_names[2]....)
	vector<string> column_names;

public:
	bool IsQualified() const;
	const string &GetColumnName() const;
	const string &GetTableName() const;
	bool IsScalar() const override {
		return false;
	}

	string GetName() const override;
	string ToString() const override;

	static bool Equal(const ColumnRefExpression &a, const ColumnRefExpression &b);
	hash_t Hash() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, FieldReader &source);
	void FormatSerialize(FormatSerializer &serializer) const override;
	static unique_ptr<ParsedExpression> FormatDeserialize(FormatDeserializer &deserializer);

private:
	ColumnRefExpression();
	//! if the column reference comes from the relational api, we need to know so we
	//! don't start quoting the column names when it isn't needed
	bool from_sql;
};
} // namespace duckdb
