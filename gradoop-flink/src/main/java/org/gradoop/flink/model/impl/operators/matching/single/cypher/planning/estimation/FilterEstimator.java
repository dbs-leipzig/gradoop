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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterEmbeddingsNode;

/**
 * Keeps track of the leaf nodes in a query plan and computes a final selectivity factor resulting
 * from the applied predicates.
 */
class FilterEstimator extends Estimator {
  /**
   * The non-filtered cardinality of the leaf node.
   */
  private long cardinality;
  /**
   * The resulting selectivity factor of all leaf predicates
   */
  private double selectivity;

  /**
   * Creates a new estimator.
   *
   * @param queryHandler query handler
   * @param graphStatistics graph statistics
   */
  FilterEstimator(QueryHandler queryHandler, GraphStatistics graphStatistics) {
    super(queryHandler, graphStatistics);
    this.selectivity = 1f;
  }

  /**
   * Updates the selectivity factor according to the given node.
   *
   * @param node leaf node
   */
  void visit(FilterNode node) {
    if (node instanceof FilterAndProjectVerticesNode) {
      FilterAndProjectVerticesNode vertexNode = (FilterAndProjectVerticesNode) node;
      setCardinality(vertexNode.getEmbeddingMetaData().getVertexVariables().get(0), true);
      updateSelectivity(vertexNode.getFilterPredicate());
    } else if (node instanceof FilterAndProjectEdgesNode) {
      FilterAndProjectEdgesNode edgeNode = (FilterAndProjectEdgesNode) node;
      setCardinality(edgeNode.getEmbeddingMetaData().getEdgeVariables().get(0), false);
      updateSelectivity(edgeNode.getFilterPredicate());
    } else if (node instanceof FilterEmbeddingsNode) {
      updateSelectivity(((FilterEmbeddingsNode) node).getFilterPredicate());
    }
  }

  /**
   * Returns the non-filtered cardinality of the leaf node output.
   *
   * @return estimated cardinality
   */
  long getCardinality() {
    return cardinality;
  }

  /**
   * Returns the combined selectivity of all leaf nodes.
   *
   * @return combined selectivity factor
   */
  double getSelectivity() {
    return selectivity;
  }

  /**
   * Updates the cardinality of the leaf node output.
   *
   * @param variable query variable
   * @param isVertex true, iff the variable maps to a vertex
   */
  private void setCardinality(String variable, boolean isVertex) {
    cardinality = getCardinality(getLabel(variable, isVertex), isVertex);
  }

  /**
   * Updates the selectivity based on the given predicates.
   *
   * @param predicates query predicates
   */
  private void updateSelectivity(CNF predicates) {
    // TODO
  }
}
