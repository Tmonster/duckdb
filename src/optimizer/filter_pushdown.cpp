#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

FilterPushdown::FilterPushdown(Optimizer &optimizer) : optimizer(optimizer), combiner(optimizer.context) {
}

//FilterPushdown::RemoveExpressions(LogicalOperator &op) {
//	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
//		auto &colref = expr.Cast<BoundColumnRefExpression>();
//		D_ASSERT(colref.depth == 0);
//
//		colref.binding.table_index = setop.table_index;
//		return;
//	}
//	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ReplaceFilterTableIndex(child, setop); });
//}

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	unique_ptr<LogicalOperator> ret;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		ret = PushdownAggregate(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		ret = PushdownFilter(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		ret = PushdownCrossProduct(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		ret = PushdownJoin(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		ret = PushdownProjection(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		ret = PushdownSetOperation(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		ret = PushdownDistinct(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		ret = std::move(op);
		break;
	}
	case LogicalOperatorType::LOGICAL_GET:
		ret = PushdownGet(std::move(op));
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		ret = PushdownLimit(std::move(op));
		break;
	default:
		ret = FinishPushdown(std::move(op));
		break;
	}
	return ret;
}

ClientContext &FilterPushdown::GetContext() {
	return optimizer.GetContext();
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	if (!join.left_projection_map.empty() || !join.right_projection_map.empty()) {
		// cannot push down further otherwise the projection maps won't be preserved
		return FinishPushdown(std::move(op));
	}

	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		return PushdownInnerJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SEMI:
	case JoinType::ANTI:
		return PushdownSemiAntiJoin(std::move(op));
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(std::move(op));
	}
}
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(std::move(f->filter));
		D_ASSERT(result != FilterResult::UNSUPPORTED);
		(void)result;
	}
	filters.clear();
}

FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &child_expr : expressions) {
		if (combiner.AddFilter(std::move(child_expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (!filters.empty()) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_uniq<Filter>();
		f->filter = std::move(filter);
		f->ExtractBindings();
		filters.push_back(std::move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::AddLogicalFilter(unique_ptr<LogicalOperator> op,
                                                             vector<unique_ptr<Expression>> expressions) {
	if (expressions.empty()) {
		// No left expressions, so needn't to add an extra filter operator.
		return op;
	}
	auto filter = make_uniq<LogicalFilter>();
	filter->expressions = std::move(expressions);
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPushdown::PushFinalFilters(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<Expression>> expressions;
	for (auto &f : filters) {
		expressions.push_back(std::move(f->filter));
	}

	return AddLogicalFilter(std::move(op), std::move(expressions));
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer);
		child = pushdown.Rewrite(std::move(child));
	}
	// now push any existing filters
	return PushFinalFilters(std::move(op));
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb
