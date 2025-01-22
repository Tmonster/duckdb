#include "duckdb/optimizer/join_order/relation_manager.hpp"

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <valarray>

namespace duckdb {

const vector<RelationStats> RelationManager::GetRelationStats() {
	vector<RelationStats> ret;
	for (idx_t i = 0; i < relations.size(); i++) {
		ret.push_back(relations[i]->stats);
	}
	return ret;
}

vector<unique_ptr<SingleJoinRelation>> RelationManager::GetRelations() {
	return std::move(relations);
}

idx_t RelationManager::NumRelations() {
	return relations.size();
}

void FilterInfo::SetLeftSet(optional_ptr<JoinRelationSet> left_set_new) {
	left_relation_set = left_set_new;
}

void FilterInfo::SetRightSet(optional_ptr<JoinRelationSet> right_set_new) {
	right_relation_set = right_set_new;
}

bool FilterInfo::SingleColumnFilter() {
	return left_relation_set->Empty() || right_relation_set->Empty();
}

void RelationManager::AddAggregateOrWindowRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                                   const RelationStats &stats, LogicalOperatorType op_type) {
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto op_bindings = op.GetColumnBindings();
	for (auto &binding : op_bindings) {
		if (relation_mapping.find(binding.table_index) == relation_mapping.end()) {
			relation_mapping[binding.table_index] = relation_id;
		}
	}
	relations.push_back(std::move(relation));
	op.estimated_cardinality = stats.cardinality;
	op.has_estimated_cardinality = true;
}

void RelationManager::AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                  const RelationStats &stats) {

	// if parent is null, then this is a root relation
	// if parent is not null, it should have multiple children
	D_ASSERT(!parent || parent->children.size() >= 2);
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	if (table_indexes.empty()) {
		// relation represents a non-reorderable relation, most likely a join relation
		// Get the tables referenced in the non-reorderable relation and add them to the relation mapping
		// This should all table references, even if there are nested non-reorderable joins.
		unordered_set<idx_t> table_references;
		LogicalJoin::GetTableReferences(op, table_references);
		D_ASSERT(table_references.size() > 0);
		for (auto &reference : table_references) {
			D_ASSERT(relation_mapping.find(reference) == relation_mapping.end());
			relation_mapping[reference] = relation_id;
		}
	} else {
		auto bindings = op.GetColumnBindings();
		for (auto &binding : bindings) {
			if (relation_mapping.find(binding.table_index) == relation_mapping.end()) {
				relation_mapping[binding.table_index] = relation_id;
			}
		}
	}
	relations.push_back(std::move(relation));
	op.estimated_cardinality = stats.cardinality;
	op.has_estimated_cardinality = true;
}

bool RelationManager::CrossProductWithRelationAllowed(idx_t relation_id) {
	return no_cross_product_relations.find(relation_id) == no_cross_product_relations.end();
}

static bool OperatorNeedsRelation(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return true;
	default:
		return false;
	}
}

static bool OperatorIsNonReorderable(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
}

bool ExpressionContainsColumnRef(Expression &expression) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
#ifdef DEBUG
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		(void)colref.depth;
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
#endif
		// map the base table index to the relation index used by the JoinOrderOptimizer
		return true;
	}
	// TODO: handle inequality filters with functions.
	auto children_ret = false;
	ExpressionIterator::EnumerateChildren(expression,
	                                      [&](Expression &expr) { children_ret = ExpressionContainsColumnRef(expr); });
	return children_ret;
}

static bool JoinIsReorderable(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return true;
	} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::INNER:
		case JoinType::SEMI:
		case JoinType::LEFT:
		case JoinType::ANTI:
			for (auto &cond : join.conditions) {
				if (ExpressionContainsColumnRef(*cond.left) && ExpressionContainsColumnRef(*cond.right)) {
					return true;
				}
			}
			return false;
		default:
			return false;
		}
	}
	return false;
}

static bool HasNonReorderableChild(LogicalOperator &op) {
	LogicalOperator *tmp = &op;
	while (tmp->children.size() == 1) {
		if (OperatorNeedsRelation(*tmp) || OperatorIsNonReorderable(tmp->type)) {
			return true;
		}
		tmp = tmp->children[0].get();
		if (tmp->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			if (!JoinIsReorderable(*tmp)) {
				return true;
			}
		}
	}
	return tmp->children.empty();
}

static void ModifyStatsIfLimit(optional_ptr<LogicalOperator> limit_op, RelationStats &stats) {
	if (!limit_op) {
		return;
	}
	auto &limit = limit_op->Cast<LogicalLimit>();
	if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		stats.cardinality = MinValue(limit.limit_val.GetConstantValue(), stats.cardinality);
	}
}

bool RelationManager::ExtractJoinRelations(JoinOrderOptimizer &optimizer, LogicalOperator &input_op,
                                           vector<reference<LogicalOperator>> &filter_operators,
                                           optional_ptr<LogicalOperator> parent) {
	optional_ptr<LogicalOperator> op = &input_op;
	vector<reference<LogicalOperator>> datasource_filters;
	optional_ptr<LogicalOperator> limit_op = nullptr;
	// pass through single child operators
	while (op->children.size() == 1 && !OperatorNeedsRelation(*op)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (HasNonReorderableChild(*op)) {
				datasource_filters.push_back(*op);
			}
			filter_operators.push_back(*op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
			limit_op = op;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (OperatorIsNonReorderable(op->type)) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		if (JoinIsReorderable(*op)) {
			// extract join conditions from inner join
			filter_operators.push_back(*op);
		} else {
			non_reorderable_operation = true;
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		// stats.cardinality will be initiated to highest cardinality of the children.
		vector<RelationStats> children_stats;
		for (auto &child : op->children) {
			auto stats = RelationStats();
			auto child_optimizer = optimizer.CreateChildOptimizer();
			child = child_optimizer.Optimize(std::move(child), &stats);
			children_stats.push_back(stats);
		}

		auto combined_stats = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(*op, children_stats);
		op->SetEstimatedCardinality(combined_stats.cardinality);
		if (!datasource_filters.empty()) {
			combined_stats.cardinality = (idx_t)MaxValue(
			    double(combined_stats.cardinality) * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, combined_stats);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// optimize children
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &aggr = op->Cast<LogicalAggregate>();
		auto operator_stats = RelationStatisticsHelper::ExtractAggregationStats(aggr, child_stats);
		// the extracted cardinality should be set for aggregate
		aggr.SetEstimatedCardinality(operator_stats.cardinality);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                     RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &window = op->Cast<LogicalWindow>();
		auto operator_stats = RelationStatisticsHelper::ExtractWindowStats(window, child_stats);
		// the extracted cardinality should be set for window
		window.SetEstimatedCardinality(operator_stats.cardinality);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(operator_stats.cardinality) *
			                                                     RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddAggregateOrWindowRelation(input_op, parent, operator_stats, op->type);
		return true;
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		// optimize children of unnest
		RelationStats child_stats;
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		// the extracted cardinality should be set for window
		if (!datasource_filters.empty()) {
			child_stats.cardinality = LossyNumericCast<idx_t>(static_cast<double>(child_stats.cardinality) *
			                                                  RelationStatisticsHelper::DEFAULT_SELECTIVITY);
		}
		ModifyStatsIfLimit(limit_op.get(), child_stats);
		AddRelation(input_op, parent, child_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op->Cast<LogicalComparisonJoin>();
		// Adding relations of the left side to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(optimizer, *op->children[0], filter_operators, op);
		bool can_reorder_right = true;
		// For semi/anti/left joins, you can only reorder relations through the left side of the join
		// SEMI/ANTI: If we reorder a relation A into the right side then all column bindings from A
		// will be lost after the semi or anti join
		// We cannot reorder a relation B out of the right side because any filter/join in the right side
		// between a relation B and another RHS relation will be invalid.
		// LEFT JOINS: If you push a relation A into the RHS of a left join, all LHS tuples of the left join
		// are propageted up
		// If you pull a relation A out of the RHS of a left join, you filter the LHS of the left join too strictly.

		// So we treat the right side of left join as its own relation so no relations
		// are pushed into the right side, or taken out of the right side.
		if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI || join.join_type == JoinType::LEFT) {
			RelationStats child_stats;
			// optimize the child and copy the stats
			auto child_optimizer = optimizer.CreateChildOptimizer();
			op->children[1] = child_optimizer.Optimize(std::move(op->children[1]), &child_stats);
			AddRelation(*op->children[1], op, child_stats);
			// remember that if a cross product needs to be forced, it cannot be forced
			// across the children of a semi/anti/left join
			no_cross_product_relations.insert(relations.size() - 1);
			auto right_child_bindings = op->children[1]->GetColumnBindings();
			for (auto &bindings : right_child_bindings) {
				relation_mapping[bindings.table_index] = relations.size() - 1;
			}
		} else {
			can_reorder_right = ExtractJoinRelations(optimizer, *op->children[1], filter_operators, op);
		}
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		bool can_reorder_left = ExtractJoinRelations(optimizer, *op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(optimizer, *op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		auto &dummy_scan = op->Cast<LogicalDummyScan>();
		auto stats = RelationStatisticsHelper::ExtractDummyScanStats(dummy_scan, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		auto &expression_get = op->Cast<LogicalExpressionGet>();
		auto stats = RelationStatisticsHelper::ExtractExpressionGetStats(expression_get, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		auto &get = op->Cast<LogicalGet>();
		auto stats = RelationStatisticsHelper::ExtractGetStats(get, context);
		// if there is another logical filter that could not be pushed down into the
		// table scan, apply another selectivity.
		get.SetEstimatedCardinality(stats.cardinality);
		if (!datasource_filters.empty()) {
			stats.cardinality =
			    (idx_t)MaxValue(double(stats.cardinality) * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		ModifyStatsIfLimit(limit_op.get(), stats);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		RelationStats child_stats;
		// optimize the child and copy the stats
		auto child_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = child_optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &proj = op->Cast<LogicalProjection>();
		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStatisticsHelper::ExtractProjectionStats(proj, child_stats);
		proj.SetEstimatedCardinality(proj_stats.cardinality);
		ModifyStatsIfLimit(limit_op.get(), proj_stats);
		AddRelation(input_op, parent, proj_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		// optimize the child and copy the stats
		auto &empty_result = op->Cast<LogicalEmptyResult>();
		// Projection can create columns so we need to add them here
		auto stats = RelationStatisticsHelper::ExtractEmptyResultStats(empty_result);
		empty_result.SetEstimatedCardinality(stats.cardinality);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		RelationStats lhs_stats;
		// optimize the lhs child and copy the stats
		auto lhs_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = lhs_optimizer.Optimize(std::move(op->children[0]), &lhs_stats);
		// optimize the rhs child
		auto rhs_optimizer = optimizer.CreateChildOptimizer();
		auto table_index = op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE
		                       ? op->Cast<LogicalMaterializedCTE>().table_index
		                       : op->Cast<LogicalRecursiveCTE>().table_index;
		rhs_optimizer.AddMaterializedCTEStats(table_index, std::move(lhs_stats));
		op->children[1] = rhs_optimizer.Optimize(std::move(op->children[1]));
		return false;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		if (cte_ref.materialized_cte != CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			return false;
		}
		auto cte_stats = optimizer.GetMaterializedCTEStats(cte_ref.cte_index);
		cte_ref.SetEstimatedCardinality(cte_stats.cardinality);
		AddRelation(input_op, parent, cte_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		auto &delim_join = op->Cast<LogicalComparisonJoin>();

		// optimize LHS (duplicate-eliminated) child
		RelationStats lhs_stats;
		auto lhs_optimizer = optimizer.CreateChildOptimizer();
		op->children[0] = lhs_optimizer.Optimize(std::move(op->children[0]), &lhs_stats);

		// create dummy aggregation for the duplicate elimination
		auto dummy_aggr = make_uniq<LogicalAggregate>(DConstants::INVALID_INDEX - 1, DConstants::INVALID_INDEX,
		                                              vector<unique_ptr<Expression>>());
		for (auto &delim_col : delim_join.duplicate_eliminated_columns) {
			dummy_aggr->groups.push_back(delim_col->Copy());
		}
		auto lhs_delim_stats = RelationStatisticsHelper::ExtractAggregationStats(*dummy_aggr, lhs_stats);

		// optimize the other child, which will now have access to the stats
		RelationStats rhs_stats;
		auto rhs_optimizer = optimizer.CreateChildOptimizer();
		rhs_optimizer.AddDelimScanStats(lhs_delim_stats);
		op->children[1] = rhs_optimizer.Optimize(std::move(op->children[1]), rhs_stats);

		return false;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		// Used to not be possible to reorder these. We added reordering (without stats) before,
		// but ran into terrible join orders (see internal issue #596), so we removed it again
		// We now have proper statistics for DelimGets, and get an even better query plan for #596
		auto delim_scan_stats = optimizer.GetDelimScanStats();
		op->SetEstimatedCardinality(delim_scan_stats.cardinality);
		AddAggregateOrWindowRelation(input_op, parent, delim_scan_stats, op->type);
		return true;
	}
	default:
		return false;
	}
}

void RelationManager::GetColumnBindingsFromExpression(Expression &expression, column_binding_set_t &column_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// only add column bindings that map to relations.
		if (relation_mapping.find(colref.binding.table_index) == relation_mapping.end()) {
			return;
		}
		// map the base table index to the relation index used by the JoinOrderOptimizer
		column_bindings.insert(
		    ColumnBinding(relation_mapping[colref.binding.table_index], colref.binding.column_index));
	}

	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(
	    expression, [&](Expression &expr) { GetColumnBindingsFromExpression(expr, column_bindings); });
}

optional_ptr<JoinRelationSet> RelationManager::GetJoinRelations(column_binding_set_t &column_bindings,
                                                                JoinRelationSetManager &set_manager) {
	optional_ptr<JoinRelationSet> ret = set_manager.GetEmptyJoinRelationSet();
	for (auto &binding : column_bindings) {
		optional_ptr<JoinRelationSet> binding_set = set_manager.GetJoinRelation(binding.table_index);
		ret = set_manager.Union(*ret, *binding_set);
	}
	return *ret;
}

void RelationManager::ExtractColumnBindingsFromExpression(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		if (expression.GetAlias() == "SUBQUERY" &&
		    relation_mapping.find(colref.binding.table_index) == relation_mapping.end()) {
			// most likely a BoundSubqueryExpression that was created from an uncorrelated subquery
			// Here we return true and don't fill the bindings, the expression can be reordered.
			// A filter will be created using this expression, and pushed back on top of the parent
			// operator during plan reconstruction
			return;
		}
		if (relation_mapping.find(colref.binding.table_index) != relation_mapping.end()) {
			bindings.insert(relation_mapping[colref.binding.table_index]);
		}
	}
	if (expression.GetExpressionType() == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return;
	}
	D_ASSERT(expression.GetExpressionType() != ExpressionType::SUBQUERY);
	ExpressionIterator::EnumerateChildren(
	    expression, [&](Expression &expr) { ExtractColumnBindingsFromExpression(expr, bindings); });
}

vector<unique_ptr<Expression>>
RelationManager::CreateFilterFromConjunctionChildren(unique_ptr<BoundConjunctionExpression> conjunction_expression,
                                                     JoinRelationSetManager &set_manager, JoinType join_type) {
	column_binding_set_t left_bindings, right_bindings;
	vector<unique_ptr<Expression>> leftover_expressions;
	unique_ptr<FilterInfo> filter_info = nullptr;
	// gather all relations/bindings from left expressions and all relations/bindings from the right expression sides
	// this encapsulates filters like (t1.a = t2.b OR t1.c = t2.d) to be a join condition
	// also LEFT/SEMI/ANTI joins with multiple conditions.
	for (auto &bound_expr : conjunction_expression->children) {
		if (bound_expr->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comp = bound_expr->Cast<BoundComparisonExpression>();
			GetColumnBindingsFromExpression(*comp.left, left_bindings);
			GetColumnBindingsFromExpression(*comp.right, right_bindings);
		} else {
			// if the condition is (t1.a = t2.b OR t1.c = t2.d or t1 is not null)
			// then you need t1 and t2 in the whole relation set, so add t1 and t2 to both side.
			// TODO: no you don't. But it does need to be in the left or the right.
			GetColumnBindingsFromExpression(*bound_expr, left_bindings);
			GetColumnBindingsFromExpression(*bound_expr, right_bindings);
		}
	}
	if (left_bindings.empty() && right_bindings.empty()) {
		// the conjunction filter cannot be made into a connection
		// in this case we do not create a FilterInfo for it, it will be pushed down the plan
		// during plan reconstruction. (duckdb-internal/#1493)s
		leftover_expressions.push_back(std::move(conjunction_expression));
		return leftover_expressions;
	}
	auto left_relations = GetJoinRelations(left_bindings, set_manager);
	auto right_relations = GetJoinRelations(right_bindings, set_manager);
	optional_ptr<JoinRelationSet> all_relations = set_manager.Union(*left_relations, *right_relations);
	D_ASSERT(left_relations && right_relations && all_relations && conjunction_expression);
	if (left_relations->Empty() || right_relations->Empty()) {
		filter_info = make_uniq<FilterInfo>(std::move(conjunction_expression), all_relations.get(),
		                                    filter_infos_.size(), join_type, *left_relations, *right_relations);
	} else {
		filter_info = make_uniq<FilterInfo>(std::move(conjunction_expression), all_relations.get(),
		                                    filter_infos_.size(), join_type, *left_relations, *right_relations,
		                                    *left_bindings.begin(), *right_bindings.begin());
	}
	filter_infos_.push_back(std::move(filter_info));
	return leftover_expressions;
}

vector<unique_ptr<Expression>> RelationManager::CreateFilterInfoFromExpression(unique_ptr<Expression> expr,
                                                                               JoinRelationSetManager &set_manager,
                                                                               JoinType join_type) {
	// Given a filter expression operator, check the following
	// if join_type == JoinType::LEFT, JoinType::ANTI, JoinType::SEMI,
	// 	-> treat expression as conjunction OR so and conditions don't get split up.
	// if conjunction AND - > recurse on each child
	// if conjunction OR -> extract bindings from left and right sides and create FilterInfo
	// if comparison expression -> extract relations from left and right. If both sides have a RelationSet, create
	// filter info, otherwise create a "leftover_expression" else -> create a "leftover expression"
	vector<unique_ptr<Expression>> leftover_expressions;
	unique_ptr<FilterInfo> new_filter = nullptr;
	column_binding_set_t left_bindings, right_bindings;
	optional_ptr<JoinRelationSet> left_set = nullptr;
	optional_ptr<JoinRelationSet> right_set = nullptr;
	optional_ptr<JoinRelationSet> set = nullptr;
	unique_ptr<Expression> new_expression = nullptr;
	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::LEFT: {
		// todo handle a case like select * from a join b on a.col1 = a.col2 (i.e no conditions no table b)
		// for SEMI ANTI AND LEFT, you want to keep the expressions together
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_CONJUNCTION);
		if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION) {
			auto conj = unique_ptr_cast<Expression, BoundConjunctionExpression>(std::move(expr));
			auto unused_expressions = CreateFilterFromConjunctionChildren(std::move(conj), set_manager, join_type);
			// there should not be any unused expressions here.
			D_ASSERT(unused_expressions.empty());
			D_ASSERT(!filter_infos_.empty());
			// We can guarantee there is a filterr info since the filter is created from a semi anti condition.
			auto &new_filter = *filter_infos_.back();
			left_set = new_filter.left_relation_set;
			right_set = new_filter.right_relation_set;
			set = set_manager.Union(*left_set, *right_set);
		} else {
			throw InternalException("left/semi/anti join should have conjunction expression");
		}

		if (join_type == JoinType::LEFT) {
			// When we extract relations from the left join, all filters already extracted (i.e above the left
			// join) must be checked for the following condition If the filter includes relations from the RHS
			// of the LEFT JOIN, then all LHS relations of the LEFT JOIN are required to be present before the
			// filter can take place. This means the left join will be planned before the filter.
			for (auto &filter : filter_infos_) {
				if (filter->join_type == JoinType::LEFT) {
					// don't inspect the filter we just created.
					continue;
				}
				// if any filter filters on just the right set,
				// if there is no right set, it is a single column filter?
				if (JoinRelationSet::IsSubset(*filter->set, *right_set)) {
					// TODO: I don't think changing the set does much, you need to change the left and right set and
					//  the conditions of what is required.
					// make sure it requires all relations from the left set.
					// if the filter is a (T1.a = 9) where t1.a is in the RHS of the left join
					// then the filter set needs the left relations of the left join filter
					// if the filter is a (T1.a = T2.b) where T1.a is the RHS of the left join and t2.b is the
					// LHS, then we are fine. Union the two sets because if you have a join plan like so ((A
					// LEFT JOIN B) JOIN C) with condition B.x = C.y the upper inner join has the total set (A,
					// B, C).
					filter->set = set_manager.Union(*filter->set, *set);
				}
				if (JoinRelationSet::IsSubset(*filter->left_relation_set, *right_set)) {
					filter->left_relation_set = set_manager.Union(*filter->left_relation_set, *set);
				}
				if (JoinRelationSet::IsSubset(*filter->right_relation_set, *right_set)) {
					filter->right_relation_set = set_manager.Union(*filter->right_relation_set, *set);
				}
			}
		}
		// there really should be no leftover expressions
		D_ASSERT(leftover_expressions.empty());
		break;
	}
	// for inner joins, the filter condition can come from regular filter operation
	// so we want to extract each expressions individually (if it's a conjunction and)
	case JoinType::INNER: {
		if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION) {
			auto conj = unique_ptr_cast<Expression, BoundConjunctionExpression>(std::move(expr));
			if (conj->type == ExpressionType::CONJUNCTION_AND) {
				// recurse into conjunction children and try to make join filter from each child.
				for (idx_t i = 0; i < conj->children.size(); i++) {
					auto child = std::move(conj->children[i]);
					auto unused_expressions = CreateFilterInfoFromExpression(std::move(child), set_manager, join_type);
					for (idx_t j = 0; j < unused_expressions.size(); j++) {
						auto unused_expression = std::move(unused_expressions[j]);
						if (unused_expression) {
							leftover_expressions.push_back(std::move(unused_expression));
						}
					}
				}
			} else {
				auto unused_expressions = CreateFilterFromConjunctionChildren(std::move(conj), set_manager, join_type);
				for (idx_t j = 0; j < unused_expressions.size(); j++) {
					auto unused_expression = std::move(unused_expressions[j]);
					if (unused_expression) {
						leftover_expressions.push_back(std::move(unused_expression));
					}
				}
				return leftover_expressions;
			}
		} else if (expr->expression_class == ExpressionClass::BOUND_COMPARISON) {
			auto &comp = expr->Cast<BoundComparisonExpression>();
			auto new_comp =
			    make_uniq<BoundComparisonExpression>(comp.type, std::move(comp.left), std::move(comp.right));
			GetColumnBindingsFromExpression(*new_comp->left, left_bindings);
			GetColumnBindingsFromExpression(*new_comp->right, right_bindings);
			left_set = GetJoinRelations(left_bindings, set_manager);
			right_set = GetJoinRelations(right_bindings, set_manager);
			set = set_manager.Union(*left_set, *right_set);
			new_expression = unique_ptr_cast<BoundComparisonExpression, Expression>(std::move(new_comp));
		} else {
			// filter is something like `t1.a is null` or `not t1.a`
			// or t1.a IN (1, 4, 9)
			GetColumnBindingsFromExpression(*expr, left_bindings);
			left_set = GetJoinRelations(left_bindings, set_manager);
			right_set = GetJoinRelations(right_bindings, set_manager);
			set = set_manager.Union(*left_set, *right_set);
			new_expression = std::move(expr);
		}
		break;
	}
	default:
		throw InternalException("Unknown join type");
	}

	if (left_bindings.empty() && right_bindings.empty()) {
		// the filter cannot be made into a connection
		// in this case we do not create a FilterInfo for it, it will be pushed down the plan
		// during plan reconstruction. (duckdb-internal/#1493)s
		if (new_expression) {
			leftover_expressions.push_back(std::move(new_expression));
		}
	} else if (new_expression) {
		D_ASSERT(new_expression);
		D_ASSERT(set && left_set && right_set);
		if (left_set->Empty() || right_set->Empty()) {
			new_filter = make_uniq<FilterInfo>(std::move(new_expression), set.get(), filter_infos_.size(), join_type,
			                                   *left_set, *right_set);
		} else {
			new_filter = make_uniq<FilterInfo>(std::move(new_expression), set.get(), filter_infos_.size(), join_type,
			                                   *left_set, *right_set, *left_bindings.begin(), *right_bindings.begin());
		}
		filter_infos_.push_back(std::move(new_filter));
	}
	return leftover_expressions;
}

vector<unique_ptr<FilterInfo>> RelationManager::ExtractEdges(vector<reference<LogicalOperator>> &filter_operators,
                                                             JoinRelationSetManager &set_manager) {
	D_ASSERT(filter_infos_.empty());
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	expression_set_t filter_set;
	for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
		if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    f_op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &join = f_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.expressions.empty());
			switch (join.join_type) {
			case JoinType::SEMI:
			case JoinType::ANTI:
			case JoinType::LEFT: {
				// create a filter info that is a conjunction of all conditions, you cannot split up
				// conditions for these join types.
				unique_ptr<BoundConjunctionExpression> conj_expr =
				    make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
				for (idx_t i = 0; i < join.conditions.size(); i++) {
					auto &condition = join.conditions[i];
					auto expr = make_uniq<BoundComparisonExpression>(condition.comparison, std::move(condition.left),
					                                                 std::move(condition.right));
					conj_expr->children.push_back(std::move(expr));
				}
				auto leftover_exprs = CreateFilterInfoFromExpression(std::move(conj_expr), set_manager, join.join_type);
				D_ASSERT(leftover_exprs.empty());
				break;
			}
			default: {
				D_ASSERT(join.join_type == JoinType::INNER);
				for (idx_t i = 0; i < join.conditions.size(); i++) {
					auto &condition = join.conditions[i];
					auto expr = make_uniq<BoundComparisonExpression>(condition.comparison, std::move(condition.left),
					                                                 std::move(condition.right));
					auto leftover_exprs = CreateFilterInfoFromExpression(std::move(expr), set_manager, join.join_type);
					// since this is a join, there should not be leftover expressions
					// TODO: handle a case like select * from t1 JOIN t2 on t1.a = t1.b; (i.e filter is only on t1)
					D_ASSERT(leftover_exprs.empty());
				}
				break;
			}
			}
			join.conditions.clear();
		} else {
			// handle filters from logical filters
			vector<unique_ptr<Expression>> leftover_expressions;
			for (idx_t i = 0; i < f_op.expressions.size(); i++) {
				auto expression = std::move(f_op.expressions[i]);
				auto l = CreateFilterInfoFromExpression(std::move(expression), set_manager, JoinType::INNER);
				for (idx_t j = 0; j < l.size(); j++) {
					auto expr = std::move(l[j]);
					leftover_expressions.push_back(std::move(expr));
				}
			}
			f_op.expressions = std::move(leftover_expressions);
		}
	}
	for (auto &filter : filter_infos_) {
		D_ASSERT(!filter->set->Empty());
	}
	return std::move(filter_infos_);
}

// LCOV_EXCL_START

void RelationManager::PrintRelationStats() {
#ifdef DEBUG
	string to_print;
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &relation = relations.at(i);
		auto &stats = relation->stats;
		D_ASSERT(stats.column_names.size() == stats.column_distinct_count.size());
		for (idx_t i = 0; i < stats.column_names.size(); i++) {
			to_print = stats.column_names.at(i) + " has estimated distinct count " +
			           to_string(stats.column_distinct_count.at(i).distinct_count);
			Printer::Print(to_print);
		}
		to_print = stats.table_name + " has estimated cardinality " + to_string(stats.cardinality);
		to_print += " and relation id " + to_string(i) + "\n";
		Printer::Print(to_print);
	}
#endif
}

// LCOV_EXCL_STOP

} // namespace duckdb
