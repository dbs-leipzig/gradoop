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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterAndProjectVertices;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.LeafNode;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Leaf node that wraps a {@link FilterAndProjectVertices} operator.
 */
public class FilterAndProjectVerticesNode extends LeafNode {
  /**
   * Input data set
   */
  private DataSet<Vertex> vertices;
  /**
   * Query variable of the vertex
   */
  private final String vertexVariable;
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
   * @param vertices input vertices
   * @param vertexVariable query variable of the vertex
   * @param filterPredicate filter predicate to be applied on edges
   * @param projectionKeys property keys whose associated values are projected to the output
   */
  public FilterAndProjectVerticesNode(DataSet<Vertex> vertices, String vertexVariable,
    CNF filterPredicate, Set<String> projectionKeys) {
    this.vertices = vertices;
    this.vertexVariable = vertexVariable;
    this.filterPredicate = filterPredicate;
    this.projectionKeys = projectionKeys.stream().collect(Collectors.toList());
  }

  @Override
  public DataSet<Embedding> execute() {
    return new FilterAndProjectVertices(vertices, vertexVariable, filterPredicate, projectionKeys)
      .evaluate();
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();
    embeddingMetaData.setEntryColumn(vertexVariable, EmbeddingMetaData.EntryType.VERTEX, 0);
    embeddingMetaData = setPropertyColumns(embeddingMetaData, vertexVariable, projectionKeys);

    return embeddingMetaData;
  }

  @Override
  public String toString() {
    return String.format("FilterAndProjectVerticesNode{" +
        "vertexVariable=%s, " +
        "filterPredicate=%s, " +
        "projectionKeys=%s}",
      vertexVariable, filterPredicate, projectionKeys);
  }
}
