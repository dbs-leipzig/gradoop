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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectEdgesAlt;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.LeafNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Leaf node that wraps a {@link FilterAndProjectEdgesAlt} operator.
 */
public class FilterAndProjectEdgesAltNode extends LeafNode implements FilterNode, ProjectionNode {
  /**
   * Input data set
   */
  private DataSet<Edge> edges;
  /**
   * Query variable of the source vertex
   */
  private final String sourceVariable;
  /**
   * Query variable of the edge
   */
  private final String edgeVariable;
  /**
   * Query variable of the target vertex
   */
  private final String targetVariable;
  /**
   * Filter predicate that is applied on the input data set
   */
  private CNF filterPredicate;
  /**
   * Property keys used for projection
   */
  private final List<String> projectionKeys;

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
  public FilterAndProjectEdgesAltNode(DataSet<Edge> edges,
    String sourceVariable, String edgeVariable, String targetVariable,
    CNF filterPredicate, Set<String> projectionKeys) {
    this.edges = edges;
    this.sourceVariable = sourceVariable;
    this.edgeVariable = edgeVariable;
    this.targetVariable = targetVariable;
    this.filterPredicate = filterPredicate;
    this.projectionKeys = projectionKeys.stream().collect(Collectors.toList());
  }

  @Override
  public DataSet<Embedding> execute() {
    return new FilterAndProjectEdgesAlt(edges, edgeVariable, filterPredicate, projectionKeys)
      .evaluate();
  }

  /**
   * Returns a copy of the filter predicate attached to this node.
   *
   * @return filter predicate
   */
  public CNF getFilterPredicate() {
    return new CNF(filterPredicate);
  }

  /**
   * Returns a copy of the projection keys attached to this node.
   *
   * @return projection keys
   */
  public List<String> getProjectionKeys() {
    return new ArrayList<>(projectionKeys);
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();
    embeddingMetaData.setEntryColumn(sourceVariable, EmbeddingMetaData.EntryType.VERTEX, 0);
    embeddingMetaData.setEntryColumn(edgeVariable, EmbeddingMetaData.EntryType.EDGE, 1);
    embeddingMetaData.setEntryColumn(targetVariable, EmbeddingMetaData.EntryType.VERTEX, 2);

    embeddingMetaData = setPropertyColumns(embeddingMetaData, edgeVariable, projectionKeys);

    return embeddingMetaData;
  }

  @Override
  public String toString() {
    return String.format("FilterAndProjectEdgesNode{" +
        "sourceVariable='%s', " +
        "edgeVariable='%s', " +
        "targetVariable='%s', " +
        "filterPredicate=%s, " +
        "projectionKeys=%s}",
      sourceVariable, edgeVariable, targetVariable, filterPredicate, projectionKeys);
  }
}
