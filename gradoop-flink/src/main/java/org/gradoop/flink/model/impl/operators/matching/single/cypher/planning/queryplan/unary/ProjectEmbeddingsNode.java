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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
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
    op.setName(toString());
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
}
