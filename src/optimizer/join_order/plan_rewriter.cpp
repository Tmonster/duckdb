#include "duckdb/optimizer/join_order/plan_rewriter.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"

#include <cmath>

namespace duckdb {

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
		// we don't, we need to create one
		auto filter = make_uniq<LogicalFilter>();
		filter->children.push_back(std::move(node));
		node = std::move(filter);
	}
	// push the filter into the LogicalFilRewritePlanter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = node->Cast<LogicalFilter>();
	filter.expressions.push_back(std::move(expr));
	return node;
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == &rel.op) {
			// found it! take ownership of it from the parent
			auto result = std::move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

GenerateJoinRelation PlanRewriter::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                 JoinNode &node) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node.left && node.right && node.info) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, *node.left);
		auto right = GenerateJoins(extracted_relations, *node.right);

		if (node.info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
		} else {
			// we have filters, create a join node
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));
			// set the join conditions from the join node
			for (auto &filter_ref : node.info->filters) {
				auto &f = filter_ref.get();
				// extract the filter from the operator it originally belonged to
				D_ASSERT(join_optimizer->filters[f.filter_index]);
				auto condition = std::move(join_optimizer->filters[f.filter_index]);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(left.set, *f.left_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.right_set)) ||
				         (JoinRelationSet::IsSubset(left.set, *f.right_set) &&
				          JoinRelationSet::IsSubset(right.set, *f.left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = condition->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(left.set, *f.left_set);
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(cond.comparison);
				}
				join->conditions.push_back(std::move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = &left.set;
		right_node = &right.set;
		result_relation = &join_optimizer->set_manager.Union(*left_node, *right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node.set.count == 1);
		D_ASSERT(extracted_relations[node.set.relations[0]]);
		result_relation = &node.set;
		result_operator = std::move(extracted_relations[node.set.relations[0]]);
	}
	result_operator->estimated_props = node.estimated_props->Copy();
	result_operator->estimated_cardinality = result_operator->estimated_props->GetCardinality<idx_t>();
	result_operator->has_estimated_cardinality = true;
	if (result_operator->type == LogicalOperatorType::LOGICAL_FILTER &&
	    result_operator->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		// FILTER on top of GET, add estimated properties to both
		auto &filter_props = *result_operator->estimated_props;
		auto &child_operator = *result_operator->children[0];
		child_operator.estimated_props = make_uniq<EstimatedProperties>(filter_props.GetCardinality<double>() /
		                                                                    CardinalityEstimator::DEFAULT_SELECTIVITY,
		                                                                filter_props.GetCost<double>());
		child_operator.estimated_cardinality = child_operator.estimated_props->GetCardinality<idx_t>();
		child_operator.has_estimated_cardinality = true;
	}
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : join_optimizer->filter_infos) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (join_optimizer->filters[info.filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info.set.count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto filter = std::move(join_optimizer->filters[info.filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info.left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(*left_node, *info.left_set) &&
				    JoinRelationSet::IsSubset(*right_node, *info.right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(*right_node, *info.left_set) &&
				           JoinRelationSet::IsSubset(*left_node, *info.right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = filter->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(std::move(node->children[0]));
					comp_join->children.push_back(std::move(node->children[1]));
					comp_join->conditions.push_back(std::move(cond));
					if (node == result_operator.get()) {
						result_operator = std::move(comp_join);
					} else {
						D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
						result_operator->children[0] = std::move(comp_join);
					}
				} else {
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
					         node->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);
					auto &comp_join = node->Cast<LogicalComparisonJoin>();
					comp_join.conditions.push_back(std::move(cond));
				}
			}
		}
	}
	return GenerateJoinRelation(*result_relation, std::move(result_operator));
}

unique_ptr<LogicalOperator> PlanRewriter::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;

	extracted_relations.reserve(join_optimizer->relations.size());
	for (auto &relation : join_optimizer->relations) {
		extracted_relations.push_back(ExtractJoinRelation(*relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : join_optimizer->filters) {
		// check if the filter has already been extracted
		if (filter) {
			// if not we need to push it
			join_tree.op = PushFilter(std::move(join_tree.op), std::move(filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return std::move(join_tree.op);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	       op->type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = std::move(join_tree.op);
	return std::move(plan);
}

}
