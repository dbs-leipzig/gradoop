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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.leaf;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaDataFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectEdges;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;

import java.util.Set;

/**
 * Leaf node that wraps a {@link FilterAndProjectEdges} operator.
 */
public class FilterAndProjectEdgesNode implements LeafNode {
  /**
   * Input data set
   */
  private DataSet<Edge> edges;
  /**
   * Filter predicate that is applied on the input data set
   */
  private CNF filterPredicate;
  /**
   * Meta data describing the output of that node
   */
  private EmbeddingMetaData embeddingMetaData;

  /**
   * Creates a new node.
   *
   * @param edges input edges
   * @param sourceVariable query variable of the source vertex
   * @param edgeVariable query variable of the edge
   * @param targetVariable query variable of the target vertex
   * @param filterPredicate filter predicate to be applied on edges
   * @param projectionKeys property keys whose associated values are projected to the output
   */
  public FilterAndProjectEdgesNode(DataSet<Edge> edges,
    String sourceVariable, String edgeVariable, String targetVariable,
    CNF filterPredicate, Set<String> projectionKeys) {
    this.edges = edges;
    this.filterPredicate = filterPredicate;
    this.embeddingMetaData = EmbeddingMetaDataFactory.forFilterAndProjectEdges(
      sourceVariable, edgeVariable, targetVariable, Lists.newArrayList(projectionKeys));
  }

  @Override
  public DataSet<Embedding> execute() {
    return new FilterAndProjectEdges(edges, filterPredicate, embeddingMetaData).evaluate();
  }

  @Override
  public Estimator getEstimator() {
    return null;
  }

  @Override
  public EmbeddingMetaData getEmbeddingMetaData() {
    return embeddingMetaData;
  }
}
