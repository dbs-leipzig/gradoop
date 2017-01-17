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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.unary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaDataFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.UnaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Unary node that wraps a {@link ProjectEmbeddings} operator.
 */
public class ProjectEmbeddingsNode implements UnaryNode {
  /**
   * Input plan node
   */
  private final PlanNode childNode;
  /**
   * Meta data describing the output of that node
   */
  private final EmbeddingMetaData embeddingMetaData;
  /**
   * Property columns that are taken over to the output embedding
   */
  private final List<Integer> whiteListColumns;

  /**
   * Creates new node.
   *
   * @param childNode input plan node
   * @param projectedKeys property keys whose associated values are projected to the output
   */
  public ProjectEmbeddingsNode(PlanNode childNode, List<Pair<String, String>> projectedKeys) {
    this.childNode = childNode;
    EmbeddingMetaData childMetaData = childNode.getEmbeddingMetaData();
    // compute output meta data
    this.embeddingMetaData = EmbeddingMetaDataFactory
      .forProjectEmbeddings(childMetaData, projectedKeys);
    // compute columns of projected properties
    whiteListColumns = projectedKeys.stream()
      .map(pair -> childMetaData.getPropertyColumn(pair.getLeft(), pair.getRight()))
      .collect(Collectors.toList());
  }

  @Override
  public PlanNode getChild() {
    return childNode;
  }

  @Override
  public DataSet<Embedding> execute() {
    return new ProjectEmbeddings(childNode.execute(), whiteListColumns).evaluate();
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
