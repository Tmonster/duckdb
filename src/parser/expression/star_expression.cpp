#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/common/exception.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

StarExpression::StarExpression(string relation_name_p)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), relation_name(std::move(relation_name_p)) {
}

string StarExpression::ToString() const {
	string result;
	if (unpacked) {
		D_ASSERT(columns);
		result += "*";
	}
	if (expr) {
		D_ASSERT(columns);
		result += "COLUMNS(" + expr->ToString() + ")";
		return result;
	}
	if (columns) {
		result += "COLUMNS(";
	}
	result += relation_name.empty() ? "*" : relation_name + ".*";
	if (!exclude_list.empty()) {
		result += " EXCLUDE (";
		bool first_entry = true;
		for (auto &entry : exclude_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.ToString();
			first_entry = false;
		}
		result += ")";
	}
	if (!replace_list.empty()) {
		result += " REPLACE (";
		bool first_entry = true;
		for (auto &entry : replace_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.second->ToString();
			result += " AS ";
			result += KeywordHelper::WriteOptionallyQuoted(entry.first);
			first_entry = false;
		}
		result += ")";
	}
	if (!rename_list.empty()) {
		result += " RENAME (";
		bool first_entry = true;
		for (auto &entry : rename_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.first.ToString();
			result += " AS ";
			result += KeywordHelper::WriteOptionallyQuoted(entry.second);
			first_entry = false;
		}
		result += ")";
	}
	if (columns) {
		result += ")";
	}
	return result;
}

bool StarExpression::Equal(const StarExpression &a, const StarExpression &b) {
	if (a.relation_name != b.relation_name || a.exclude_list != b.exclude_list || a.rename_list != b.rename_list) {
		return false;
	}
	if (a.columns != b.columns) {
		return false;
	}
	if (a.unpacked != b.unpacked) {
		return false;
	}
	if (a.replace_list.size() != b.replace_list.size()) {
		return false;
	}
	for (auto &entry : a.replace_list) {
		auto other_entry = b.replace_list.find(entry.first);
		if (other_entry == b.replace_list.end()) {
			return false;
		}
		if (!entry.second->Equals(*other_entry->second)) {
			return false;
		}
	}
	if (!ParsedExpression::Equals(a.expr, b.expr)) {
		return false;
	}
	return true;
}

bool StarExpression::IsStar(const ParsedExpression &a) {
	if (a.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = a.Cast<StarExpression>();
	return star.columns == false;
}

bool StarExpression::IsColumns(const ParsedExpression &a) {
	if (a.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = a.Cast<StarExpression>();
	return star.columns == true && star.unpacked == false;
}

bool StarExpression::IsColumnsUnpacked(const ParsedExpression &a) {
	if (a.GetExpressionClass() != ExpressionClass::STAR) {
		return false;
	}
	auto &star = a.Cast<StarExpression>();
	return star.columns == true && star.unpacked == true;
}

unique_ptr<ParsedExpression> StarExpression::Copy() const {
	auto copy = make_uniq<StarExpression>(relation_name);
	copy->exclude_list = exclude_list;
	for (auto &entry : replace_list) {
		copy->replace_list[entry.first] = entry.second->Copy();
	}
	copy->rename_list = rename_list;
	copy->columns = columns;
	copy->expr = expr ? expr->Copy() : nullptr;
	copy->CopyProperties(*this);
	copy->unpacked = unpacked;
	return std::move(copy);
}

StarExpression::StarExpression(const case_insensitive_set_t &exclude_list_p, qualified_column_set_t qualified_set)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), exclude_list(std::move(qualified_set)) {
	for (auto &entry : exclude_list_p) {
		exclude_list.insert(QualifiedColumnName(entry));
	}
}

case_insensitive_set_t StarExpression::SerializedExcludeList() const {
	// we serialize non-qualified elements in a separate list of only column names for backwards compatibility
	case_insensitive_set_t result;
	for (auto &entry : exclude_list) {
		if (!entry.IsQualified()) {
			result.insert(entry.column);
		}
	}
	return result;
}

qualified_column_set_t StarExpression::SerializedQualifiedExcludeList() const {
	// we serialize only qualified elements in the qualified list for backwards compatibility
	qualified_column_set_t result;
	for (auto &entry : exclude_list) {
		if (entry.IsQualified()) {
			result.insert(entry);
		}
	}
	return result;
}

} // namespace duckdb
