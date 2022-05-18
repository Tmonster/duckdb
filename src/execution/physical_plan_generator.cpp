#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	explicit DependencyExtractor(unordered_set<CatalogEntry *> &dependencies) : dependencies(dependencies) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// extract dependencies from the bound function expression
		if (expr.function.dependency) {
			expr.function.dependency(expr, dependencies);
		}
		return nullptr;
	}

private:
	unordered_set<CatalogEntry *> &dependencies;
};

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	auto &profiler = QueryProfiler::Get(context);

	// first resolve column references
	profiler.StartPhase("column_binding");
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	profiler.EndPhase();

	// now resolve types of all the operators
	profiler.StartPhase("resolve_types");
	op->ResolveOperatorTypes();
	profiler.EndPhase();

	// extract dependencies from the logical plan
	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(*op);

	// then create the main physical plan
	profiler.StartPhase("create_plan");
	auto plan = CreatePlan(*op);
	profiler.EndPhase();

	plan->Verify();
	plan->ph_join_stats = op->join_stats;
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &op) {
	op.estimated_cardinality = op.EstimateCardinality(context);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return CreatePlan((LogicalGet &)op);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return CreatePlan((LogicalProjection &)op);
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return CreatePlan((LogicalEmptyResult &)op);
	case LogicalOperatorType::LOGICAL_FILTER:
		return CreatePlan((LogicalFilter &)op);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return CreatePlan((LogicalAggregate &)op);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return CreatePlan((LogicalWindow &)op);
	case LogicalOperatorType::LOGICAL_UNNEST:
		return CreatePlan((LogicalUnnest &)op);
	case LogicalOperatorType::LOGICAL_LIMIT:
		return CreatePlan((LogicalLimit &)op);
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		return CreatePlan((LogicalLimitPercent &)op);
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return CreatePlan((LogicalSample &)op);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return CreatePlan((LogicalOrder &)op);
	case LogicalOperatorType::LOGICAL_TOP_N:
		return CreatePlan((LogicalTopN &)op);
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		return CreatePlan((LogicalCopyToFile &)op);
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return CreatePlan((LogicalDummyScan &)op);
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return CreatePlan((LogicalAnyJoin &)op);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return CreatePlan((LogicalDelimJoin &)op);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return CreatePlan((LogicalComparisonJoin &)op);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return CreatePlan((LogicalCrossProduct &)op);
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return CreatePlan((LogicalSetOperation &)op);
	case LogicalOperatorType::LOGICAL_INSERT:
		return CreatePlan((LogicalInsert &)op);
	case LogicalOperatorType::LOGICAL_DELETE:
		return CreatePlan((LogicalDelete &)op);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return CreatePlan((LogicalChunkGet &)op);
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return CreatePlan((LogicalDelimGet &)op);
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return CreatePlan((LogicalExpressionGet &)op);
	case LogicalOperatorType::LOGICAL_UPDATE:
		return CreatePlan((LogicalUpdate &)op);
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		return CreatePlan((LogicalCreateTable &)op);
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		return CreatePlan((LogicalCreateIndex &)op);
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		return CreatePlan((LogicalExplain &)op);
	case LogicalOperatorType::LOGICAL_SHOW:
		return CreatePlan((LogicalShow &)op);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return CreatePlan((LogicalDistinct &)op);
	case LogicalOperatorType::LOGICAL_PREPARE:
		return CreatePlan((LogicalPrepare &)op);
	case LogicalOperatorType::LOGICAL_EXECUTE:
		return CreatePlan((LogicalExecute &)op);
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		return CreatePlan((LogicalCreate &)op);
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return CreatePlan((LogicalPragma &)op);
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_LOAD:
		return CreatePlan((LogicalSimple &)op);
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return CreatePlan((LogicalRecursiveCTE &)op);
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return CreatePlan((LogicalCTERef &)op);
	case LogicalOperatorType::LOGICAL_EXPORT:
		return CreatePlan((LogicalExport &)op);
	case LogicalOperatorType::LOGICAL_SET:
		return CreatePlan((LogicalSet &)op);
	default: {
		throw NotImplementedException("Unimplemented logical operator type!");
	}
	}

//	plan->AddStats(op.join_stats);
//	switch (op.type) {
//	if (op.type == LogicalOperatorType::LOGICAL_GET)):
//		plan = CreatePlan((LogicalGet &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION)
//		plan = CreatePlan((LogicalProjection &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_EMPTY_RESULT)
//		plan = CreatePlan((LogicalEmptyResult &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_FILTER)
//		plan = CreatePlan((LogicalFilter &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY)
//		plan = CreatePlan((LogicalAggregate &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_WINDOW)
//		plan = CreatePlan((LogicalWindow &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_UNNEST)
//		plan = CreatePlan((LogicalUnnest &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_LIMIT)
//		plan = CreatePlan((LogicalLimit &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_LIMIT_PERCENT)
//		plan = CreatePlan((LogicalLimitPercent &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_SAMPLE)
//		plan = CreatePlan((LogicalSample &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY)
//		plan = CreatePlan((LogicalOrder &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_TOP_N)
//		plan = CreatePlan((LogicalTopN &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_COPY_TO_FILE)
//		plan = CreatePlan((LogicalCopyToFile &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_DUMMY_SCAN)
//		plan = CreatePlan((LogicalDummyScan &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_ANY_JOIN)
//		plan = CreatePlan((LogicalAnyJoin &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN)
//		plan = CreatePlan((LogicalDelimJoin &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
//		plan = CreatePlan((LogicalComparisonJoin &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT)
//		plan = CreatePlan((LogicalCrossProduct &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_UNION ||
//		op.type == LogicalOperatorType::LOGICAL_EXCEPT ||
//		op.type == LogicalOperatorType::LOGICAL_INTERSECT)
//		plan = CreatePlan((LogicalSetOperation &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_INSERT)
//		plan = CreatePlan((LogicalInsert &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_DELETE)
//		plan = CreatePlan((LogicalDelete &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CHUNK_GET)
//		plan = CreatePlan((LogicalChunkGet &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET)
//		plan = CreatePlan((LogicalDelimGet &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_EXPRESSION_GET)
//		plan = CreatePlan((LogicalExpressionGet &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_UPDATE)
//		plan = CreatePlan((LogicalUpdate &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE)
//		plan = CreatePlan((LogicalCreateTable &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CREATE_INDEX)
//		plan = CreatePlan((LogicalCreateIndex &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_EXPLAIN)
//		plan = CreatePlan((LogicalExplain &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_SHOW)
//		plan = CreatePlan((LogicalShow &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_DISTINCT)
//		plan = CreatePlan((LogicalDistinct &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_PREPARE)
//		plan = CreatePlan((LogicalPrepare &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_EXECUTE)
//		plan = CreatePlan((LogicalExecute &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CREATE_VIEW ||
//		op.type == LogicalOperatorType::LOGICAL_CREATE_SEQUENCE ||
//		op.type == LogicalOperatorType::LOGICAL_CREATE_SCHEMA ||
//		op.type == LogicalOperatorType::LOGICAL_CREATE_MACRO ||
//		op.type == LogicalOperatorType::LOGICAL_CREATE_TYPE)
//		plan = CreatePlan((LogicalCreate &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_PRAGMA)
//		plan = CreatePlan((LogicalPragma &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_TRANSACTION ||
//		op.type == LogicalOperatorType::LOGICAL_ALTER ||
//		op.type == LogicalOperatorType::LOGICAL_DROP ||
//		op.type == LogicalOperatorType::LOGICAL_VACUUM ||
//		op.type == LogicalOperatorType::LOGICAL_LOAD)
//		plan = CreatePlan((LogicalSimple &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE)
//		plan = CreatePlan((LogicalRecursiveCTE &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF)
//		plan = CreatePlan((LogicalCTERef &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_EXPORT)
//		plan = CreatePlan((LogicalExport &)op);
//	if (op.type == LogicalOperatorType::LOGICAL_SET)
//		plan = CreatePlan((LogicalSet &)op);
//
//	plan->AddStats(op.join_stats);
//	return plan;
//	default:
//		throw NotImplementedException("Unimplemented logical operator type!");
//	}
}

} // namespace duckdb
