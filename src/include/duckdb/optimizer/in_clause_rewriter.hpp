//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/in_clause_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class ClientContext;
class Optimizer;

struct MaybeZoneMapFilter {
	bool filter_created;
	unique_ptr<Expression> zone_map_filter;

	MaybeZoneMapFilter() : filter_created(false), zone_map_filter(nullptr) {
	}
	MaybeZoneMapFilter(bool filter_created, unique_ptr<Expression> zone_map_filter)
	    : filter_created(filter_created), zone_map_filter(std::move(zone_map_filter)) {
	}
};

class InClauseRewriter : public LogicalOperatorVisitor {
public:
	explicit InClauseRewriter(ClientContext &context, Optimizer &optimizer) : context(context), optimizer(optimizer) {
	}

	ClientContext &context;
	Optimizer &optimizer;
	unique_ptr<LogicalOperator> root;

public:
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
	MaybeZoneMapFilter CreateZoneMapFilter(BoundOperatorExpression &expr);

	unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

} // namespace duckdb
