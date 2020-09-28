/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;
import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;

/**
 * Estimates a given query plan by traversing its nodes and updating the state of specific
 * estimator implementations (e.g. for join, filter, project).
 */
public class TemporalQueryPlanEstimator {

  /**
   * The query plan to estimate
   */
  private final QueryPlan queryPlan;
  /**
   * Estimates the cardinality of the joins in the given query plan.
   */
  private final JoinTemporalEstimator joinEstimator;
  /**
   * Estimates the cardinality and selectivity of the leaf nodes.
   */
  private final FilterTemporalEstimator filterEstimator;

  /**
   * Creates a new plan estimator.
   *
   * @param queryPlan       query plan
   * @param queryHandler    query handler
   * @param graphStatistics graph statistics
   * @param cnfEstimation estimation of CNF predicates
   */
  public TemporalQueryPlanEstimator(QueryPlan queryPlan, TemporalQueryHandler queryHandler,
                                    TemporalGraphStatistics graphStatistics, CNFEstimation cnfEstimation) {
    this.queryPlan = queryPlan;
    this.joinEstimator = new JoinTemporalEstimator(queryHandler, graphStatistics);
    this.filterEstimator = new FilterTemporalEstimator(queryHandler, graphStatistics, cnfEstimation);
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
