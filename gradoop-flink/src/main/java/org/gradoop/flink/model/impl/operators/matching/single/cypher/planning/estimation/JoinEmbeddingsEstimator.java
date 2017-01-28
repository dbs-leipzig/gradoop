/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import com.google.common.collect.Lists;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.s1ck.gdl.model.Edge;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Keeps track of the joined leaf nodes in a query plan and computes a total estimated cardinality
 * for the plan.
 */
public class JoinEmbeddingsEstimator extends Estimator {
  /**
   * Maps vertex and edge variables to their estimated cardinality
   */
  private final Map<String, Long> cardinalities;
  /**
   * Maps vertex variables to their distinct counts per embedding
   */
  private final Map<String, List<Long>> distinctValues;

  /**
   * Creates a new estimator.
   *
   * @param queryHandler query handler
   * @param graphStatistics graph statistics
   */
  public JoinEmbeddingsEstimator(QueryHandler queryHandler, GraphStatistics graphStatistics) {
    super(queryHandler, graphStatistics);
    this.cardinalities = new HashMap<>();
    this.distinctValues = new HashMap<>();
  }

  /**
   * Updates the cardinalities according to the given join node.
   *
   * @param node join node
   */
  public void visit(BinaryNode node) {
    if (node.getLeftChild() instanceof LeafNode) {
      process(node.getLeftChild().getEmbeddingMetaData());
    }
    if (node.getRightChild() instanceof LeafNode) {
      process(node.getRightChild().getEmbeddingMetaData());
    }
  }

  /**
   * Computes the final cardinality according to the visited nodes.
   *
   * @return estimated cardinality
   */
  public long getCardinality() {
    long numerator = cardinalities.values().stream().reduce((i, j) -> i * j).orElse(0L);

    long denominator = distinctValues.values().stream()
      .map(list -> list.stream().sorted().collect(Collectors.toList()))
      .map(list -> list.subList(1, list.size()))
      .flatMap(Collection::stream)
      .reduce((i, j) -> i * j)
      .orElse(1L);

    return Math.round(1.0 * numerator / denominator);
  }

  /**
   * Updates the state using the information stored in the given meta data.
   *
   * @param metaData meta data from leaf node
   */
  private void process(EmbeddingMetaData metaData) {
    int entryCount = metaData.getEntryCount();
    List<String> variables = metaData.getVariables();
    if (entryCount == 1) {
      processVertex(variables.get(0));
    } else {
      String edgeVariable = variables.get(1);
      String sourceVariable = queryHandler.getVertexById(
        queryHandler.getEdgeByVariable(edgeVariable).getSourceVertexId()).getVariable();
      String targetVariable = queryHandler.getVertexById(
        queryHandler.getEdgeByVariable(edgeVariable).getTargetVertexId()).getVariable();
      processEdge(sourceVariable, variables.get(1), targetVariable);
    }
  }

  /**
   * Updates the state according to vertex statistics.
   *
   * @param vertexVariable vertex variable
   */
  private void processVertex(String vertexVariable) {
    String label = getLabel(vertexVariable, true);
    long cardinality = getCardinality(label, true);
    updateCardinality(vertexVariable, cardinality);
    updateDistinctValues(vertexVariable, cardinality);
  }

  /**
   * Updates the state according to the edge statistics.
   *
   * @param sourceVariable source vertex variable
   * @param edgeVariable edge variable
   * @param targetVariable target vertex variable
   */
  private void processEdge(String sourceVariable, String edgeVariable, String targetVariable) {
    String edgeLabel = getLabel(edgeVariable, false);

    long distinctSourceCount = graphStatistics.getDistinctSourceVertexCountByEdgeLabel(edgeLabel);
    if (distinctSourceCount == 0L) {
      distinctSourceCount = graphStatistics.getDistinctSourceVertexCount();
    }
    long distinctTargetCount = graphStatistics.getDistinctTargetVertexCountByEdgeLabel(edgeLabel);
    if (distinctTargetCount == 0L) {
      distinctTargetCount = graphStatistics.getDistinctTargetVertexCount();
    }

    Edge queryEdge = queryHandler.getEdgeByVariable(edgeVariable);
    if (queryEdge.getUpperBound() > 1) {
      // variable case: n-hop edge
      updateCardinality(edgeVariable, getPathCardinality(getCardinality(edgeLabel, false),
        queryEdge.getLowerBound(), queryEdge.getUpperBound(),
        distinctSourceCount, distinctTargetCount));
    } else {
      // static case: 1-hop edge
      updateCardinality(edgeVariable, getCardinality(edgeLabel, false));
    }
    updateDistinctValues(sourceVariable, distinctSourceCount);
    updateDistinctValues(targetVariable, distinctTargetCount);
  }

  /**
   * Estimated the total number of paths whose length is between the specified bounds.
   *
   * @param edgeCardinality cardinality of the traversed edge
   * @param lowerBound minimum path length
   * @param upperBound maximum path length
   * @param distinctSourceCount number of distinct source vertices
   * @param distinctTargetCount number of distinct target vertices
   *
   * @return estimated number of paths with a length in the given range
   */
  private long getPathCardinality(long edgeCardinality, int lowerBound, int upperBound,
    long distinctSourceCount, long distinctTargetCount) {

    double totalCardinality = 0L;
    long probability = distinctSourceCount * distinctTargetCount;

    for (int i = lowerBound; i <= upperBound ; i++) {
      totalCardinality += Math.pow(edgeCardinality, i) / Math.pow(probability, i - 1);
    }

    return Math.round(totalCardinality);
  }

  /**
   * Updates the cardinality of the variable.
   *
   * @param variable query variable
   * @param cardinality cardinality
   */
  private void updateCardinality(String variable, long cardinality) {
    cardinalities.put(variable, cardinalities.getOrDefault(variable, 1L) * cardinality);
  }

  /**
   * Updates the distinct value list of a vertex with the given count.
   *
   * @param variable vertex variable
   * @param count distinct count
   */
  private void updateDistinctValues(String variable, long count) {
    if (distinctValues.containsKey(variable)) {
      distinctValues.get(variable).add(count);
    } else {
      distinctValues.put(variable, Lists.newArrayList(count));
    }
  }
}
