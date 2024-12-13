#include "duckdb/optimizer/join_order/join_node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"

namespace duckdb {

DPJoinNode::DPJoinNode(JoinRelationSetOld &set) : set(set), info(nullptr), is_leaf(true), left_set(set), right_set(set) {
}

DPJoinNode::DPJoinNode(JoinRelationSetOld &set, optional_ptr<NeighborInfo> info, JoinRelationSetOld &left,
                       JoinRelationSetOld &right, double cost)
    : set(set), info(info), is_leaf(false), left_set(left), right_set(right), cost(cost) {
}

} // namespace duckdb
