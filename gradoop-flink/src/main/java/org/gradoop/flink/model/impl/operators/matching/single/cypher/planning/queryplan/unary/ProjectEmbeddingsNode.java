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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.ProjectionNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unary node that wraps a {@link ProjectEmbeddings} operator.
 */
public class ProjectEmbeddingsNode extends UnaryNode implements ProjectionNode {
  /**
   * Property columns that are taken over to the output embedding
   */
  private final List<Integer> whiteListColumns;
  /**
   * Property keys used for projection
   */
  private final List<Pair<String, String>> projectionKeys;

  /**
   * Creates new node.
   *
   * @param childNode input plan node
   * @param projectionKeys property keys whose associated values are projected to the output
   */
  public ProjectEmbeddingsNode(PlanNode childNode, List<Pair<String, String>> projectionKeys) {
    super(childNode);
    this.projectionKeys = projectionKeys;
    EmbeddingMetaData childMetaData = childNode.getEmbeddingMetaData();

    // compute columns of projected properties
    whiteListColumns = projectionKeys.stream()
      .map(pair -> childMetaData.getPropertyColumn(pair.getLeft(), pair.getRight()))
      .collect(Collectors.toList());
  }

  @Override
  public DataSet<Embedding> execute() {
    ProjectEmbeddings op =  new ProjectEmbeddings(getChildNode().execute(), whiteListColumns);
    op.setName(getOperatorName());
    return op.evaluate();
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    final EmbeddingMetaData childMetaData = getChildNode().getEmbeddingMetaData();

    projectionKeys.sort(Comparator.comparingInt(key ->
      childMetaData.getPropertyColumn(key.getLeft(), key.getRight())));

    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData();

    childMetaData.getVariables().forEach(var -> embeddingMetaData.setEntryColumn(
      var, childMetaData.getEntryType(var), childMetaData.getEntryColumn(var)));

    IntStream.range(0, projectionKeys.size()).forEach(i ->
      embeddingMetaData.setPropertyColumn(
        projectionKeys.get(i).getLeft(), projectionKeys.get(i).getRight(), i));

    return embeddingMetaData;
  }

  @Override
  public String toString() {
    return String.format("ProjectEmbeddingsNode{projectionKeys=%s}", projectionKeys);
  }

  /**
   * Generates the operator description
   * @return operator description
   */
  private String getOperatorName() {
    return String.format("ProjectEmbeddings(projectionKeys: %s", projectionKeys);
  }
}
