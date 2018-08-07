/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;

/**
 * Estimates a given query plan by traversing its nodes and updating the state of specific
 * estimator implementations (e.g. for join, filter, project).
 */
public class QueryPlanEstimator {
  /**
   * The query plan to estimate
   */
  private final QueryPlan queryPlan;
  /**
   * Estimates the cardinality of the joins in the given query plan.
   */
  private final JoinEstimator joinEstimator;
  /**
   * Estimates the cardinality and selectivity of the leaf nodes.
   */
  private final FilterEstimator filterEstimator;

  /**
   * Creates a new plan estimator.
   *
   * @param queryPlan query plan
   * @param queryHandler query handler
   * @param graphStatistics graph statistics
   */
  public QueryPlanEstimator(QueryPlan queryPlan, QueryHandler queryHandler,
    GraphStatistics graphStatistics) {
    this.queryPlan = queryPlan;
    this.joinEstimator = new JoinEstimator(queryHandler, graphStatistics);
    this.filterEstimator = new FilterEstimator(queryHandler, graphStatistics);
  }

  /**
   * Returns the estimated query plan.
   *
   * @return query plan
   */
  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  /**
   * Traverses the query plan and computes the estimated cardinality according to the nodes.
   *
   * @return estimated cardinality of the specified plan
   */
  public long getCardinality() {
    traversePlan(queryPlan.getRoot());

    long cardinality = joinEstimator.getCardinality();
    if (cardinality == 0) {
      // plan contains only a leaf node
      cardinality = filterEstimator.getCardinality();
    }
    double selectivity = filterEstimator.getSelectivity();

    return Math.round(cardinality * selectivity);
  }

  /**
   * Visits the node if necessary and traverses the plan further if possible.
   *
   * @param node plan node
   */
  private void traversePlan(PlanNode node) {
    if (node instanceof JoinNode) {
      this.joinEstimator.visit((JoinNode) node);
    }
    if (node instanceof FilterNode) {
      this.filterEstimator.visit((FilterNode) node);
    }

    if (node instanceof BinaryNode) {
      traversePlan(((BinaryNode) node).getLeftChild());
      traversePlan(((BinaryNode) node).getRightChild());
    }
    if (node instanceof UnaryNode) {
      traversePlan(((UnaryNode) node).getChildNode());
    }
  }
}
