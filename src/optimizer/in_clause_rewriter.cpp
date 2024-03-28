#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> InClauseRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->children.size() == 1) {
		root = std::move(op->children[0]);
		VisitOperatorExpressions(*op);
		op->children[0] = std::move(root);
	}

	for (auto &child : op->children) {
		child = Rewrite(std::move(child));
	}
	return op;
}

MaybeZoneMapFilter InClauseRewriter::CreateZoneMapFilter(BoundOperatorExpression &expr) {
	auto ret = MaybeZoneMapFilter();
	D_ASSERT(expr.type != ExpressionType::COMPARE_IN || expr.type != ExpressionType::COMPARE_NOT_IN);
	auto is_regular_in = expr.type == ExpressionType::COMPARE_IN;
	auto &compare_in = expr.Cast<BoundOperatorExpression>();
	if (compare_in.children[0]->type != ExpressionType::BOUND_COLUMN_REF) {
		return ret;
	}
	auto &column_ref = compare_in.children[0]->Cast<BoundColumnRefExpression>();
	auto column_index = column_ref.binding.column_index;
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		return ret;
	}

	//! check if all children are const expr
	bool children_constant = true;
	for (size_t i {1}; i < compare_in.children.size(); i++) {
		if (compare_in.children[i]->type != ExpressionType::VALUE_CONSTANT) {
			children_constant = false;
			break;
		}
	}
	if (!children_constant) {
		return ret;
	}
	auto &fst_const_value_expr = compare_in.children[1]->Cast<BoundConstantExpression>();
	auto &type = fst_const_value_expr.value.type();

	//! Check if values are consecutive, if yes transform them to >= <= (only for integers)
	// e.g. if we have x IN (1, 2, 3, 4, 5) we transform this into x >= 1 AND x <= 5
	if (!type.IsIntegral()) {
		return ret;
	}
	vector<hugeint_t> in_values;
	bool can_simplify_in_clause = true;
	for (idx_t i = 1; i < compare_in.children.size(); i++) {
		auto &const_value_expr = compare_in.children[i]->Cast<BoundConstantExpression>();
		if (const_value_expr.value.IsNull()) {
			can_simplify_in_clause = false;
			break;
		}
		in_values.push_back(const_value_expr.value.GetValue<hugeint_t>());
	}
	if (!can_simplify_in_clause || in_values.empty()) {
		return ret;
	}
	sort(in_values.begin(), in_values.end());

	for (idx_t in_val_idx = 1; in_val_idx < in_values.size(); in_val_idx++) {
		if (in_values[in_val_idx] - in_values[in_val_idx - 1] > 1) {
			can_simplify_in_clause = false;
			break;
		}
	}
	if (!can_simplify_in_clause) {
		return ret;
	}
	auto lower_bound = make_uniq<BoundComparisonExpression>(
	    is_regular_in ? ExpressionType::COMPARE_GREATERTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN,
	    expr.children[0]->Copy(), make_uniq<BoundConstantExpression>(Value::Numeric(type, in_values.front())));
	auto upper_bound = make_uniq<BoundComparisonExpression>(
	    is_regular_in ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_GREATERTHAN,
	    expr.children[0]->Copy(), make_uniq<BoundConstantExpression>(Value::Numeric(type, in_values.back())));
	auto conjunction = make_uniq<BoundConjunctionExpression>(is_regular_in ? ExpressionType::CONJUNCTION_AND
	                                                                       : ExpressionType::CONJUNCTION_OR);
	conjunction->children.push_back(std::move(lower_bound));
	conjunction->children.push_back(std::move(upper_bound));
	ret.zone_map_filter = std::move(conjunction);
	ret.filter_created = true;
	return ret;
}

unique_ptr<Expression> InClauseRewriter::VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.type != ExpressionType::COMPARE_IN && expr.type != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	auto can_create_zone_map = CreateZoneMapFilter(expr);
	if (can_create_zone_map.filter_created) {
		return std::move(can_create_zone_map.zone_map_filter);
	}
	D_ASSERT(root);
	auto in_type = expr.children[0]->return_type;
	bool is_regular_in = expr.type == ExpressionType::COMPARE_IN;
	bool all_scalar = true;
	// IN clause with many children: try to generate a mark join that replaces this IN expression
	// we can only do this if the expressions in the expression list are scalar
	for (idx_t i = 1; i < expr.children.size(); i++) {
		if (!expr.children[i]->IsFoldable()) {
			// non-scalar expression
			all_scalar = false;
		}
	}
	if (expr.children.size() == 2) {
		// only one child
		// IN: turn into X = 1
		// NOT IN: turn into X <> 1
		return make_uniq<BoundComparisonExpression>(is_regular_in ? ExpressionType::COMPARE_EQUAL
		                                                          : ExpressionType::COMPARE_NOTEQUAL,
		                                            std::move(expr.children[0]), std::move(expr.children[1]));
	}
	if (expr.children.size() < 6 || !all_scalar) {
		// low amount of children or not all scalar
		// IN: turn into (X = 1 OR X = 2 OR X = 3...)
		// NOT IN: turn into (X <> 1 AND X <> 2 AND X <> 3 ...)
		auto conjunction = make_uniq<BoundConjunctionExpression>(is_regular_in ? ExpressionType::CONJUNCTION_OR
		                                                                       : ExpressionType::CONJUNCTION_AND);
		for (idx_t i = 1; i < expr.children.size(); i++) {
			conjunction->children.push_back(make_uniq<BoundComparisonExpression>(
			    is_regular_in ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOTEQUAL,
			    expr.children[0]->Copy(), std::move(expr.children[i])));
		}
		return std::move(conjunction);
	}
	// IN clause with many constant children
	// generate a mark join that replaces this IN expression
	// first generate a ColumnDataCollection from the set of expressions
	vector<LogicalType> types = {in_type};
	auto collection = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(context, types);
	for (idx_t i = 1; i < expr.children.size(); i++) {
		// resolve this expression to a constant
		Value value;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *expr.children[i], value)) {
			// error while evaluating scalar
			return nullptr;
		}
		idx_t index = chunk.size();
		chunk.SetCardinality(chunk.size() + 1);
		chunk.SetValue(0, index, value);
		if (chunk.size() == STANDARD_VECTOR_SIZE || i + 1 == expr.children.size()) {
			// chunk full: append to chunk collection
			collection->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	// now generate a ChunkGet that scans this collection
	auto chunk_index = optimizer.binder.GenerateTableIndex();
	auto chunk_scan = make_uniq<LogicalColumnDataGet>(chunk_index, types, std::move(collection));

	// then we generate the MARK join with the chunk scan on the RHS
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
	join->mark_index = chunk_index;
	join->AddChild(std::move(root));
	join->AddChild(std::move(chunk_scan));
	// create the JOIN condition
	JoinCondition cond;
	cond.left = std::move(expr.children[0]);

	cond.right = make_uniq<BoundColumnRefExpression>(in_type, ColumnBinding(chunk_index, 0));
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	join->conditions.push_back(std::move(cond));
	root = std::move(join);

	// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
	unique_ptr<Expression> result =
	    make_uniq<BoundColumnRefExpression>("IN (...)", LogicalType::BOOLEAN, ColumnBinding(chunk_index, 0));
	if (!is_regular_in) {
		// NOT IN: invert
		auto invert = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
		invert->children.push_back(std::move(result));
		result = std::move(invert);
	}
	return result;
}

} // namespace duckdb
