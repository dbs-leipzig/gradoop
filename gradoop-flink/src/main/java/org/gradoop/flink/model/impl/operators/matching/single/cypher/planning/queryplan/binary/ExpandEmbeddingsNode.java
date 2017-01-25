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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.ExpandEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps an {@link ExpandEmbeddings} operator.
 */
public class ExpandEmbeddingsNode extends BinaryNode {
  /**
   * Column to expand the embedding from.
   */
  private final int expandColumn;
  /**
   * Query variable of the first vertex in the path
   */
  private final String startVariable;
  /**
   * Query variable of the variable length path
   */
  private final String pathVariable;
  /**
   * Query variable of the last vertex in the path
   */
  private final String endVariable;
  /**
   * Minimum number of path expansion steps
   */
  private final int lowerBound;
  /**
   * Maximum number of path expansion steps
   */
  private final int upperBound;
  /**
   * Column that contains the final vertex of the expansion
   */
  private final int closingColumn;
  /**
   * Direction in which to expand the embedding
   */
  private final ExpandDirection expandDirection;
  /**
   * Morphism type for vertices
   */
  private final MatchStrategy vertexStrategy;
  /**
   * Morphism type for edges
   */
  private final MatchStrategy edgeStrategy;

  /**
   * Creates a new node.
   *
   * @param leftChild left child representing the embeddings to expand
   * @param rightChild right child representing the edges to expand with
   * @param startVariable vertex variable on which to start the expansion
   * @param pathVariable variable representing the path
   * @param endVariable vertex variable on which to end the expansion
   * @param lowerBound minimum number of expansions
   * @param upperBound maximum number of expansions
   * @param expandDirection edge direction in the expansion
   * @param vertexStrategy morphism strategy for vertices
   * @param edgeStrategy morphism strategy for edges
   */
  public ExpandEmbeddingsNode(PlanNode leftChild, PlanNode rightChild,
    String startVariable, String pathVariable, String endVariable,
    int lowerBound, int upperBound, ExpandDirection expandDirection,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    super(leftChild, rightChild);
    this.pathVariable = pathVariable;
    this.startVariable = startVariable;
    this.endVariable = endVariable;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.expandDirection = expandDirection;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.expandColumn = leftChild.getEmbeddingMetaData().getEntryColumn(startVariable);
    this.closingColumn = leftChild.getEmbeddingMetaData().containsEntryColumn(endVariable) ?
      leftChild.getEmbeddingMetaData().getEntryColumn(endVariable) : -1;
  }

  @Override
  public DataSet<Embedding> execute() {
    return new ExpandEmbeddings(getLeftChild().execute(), getRightChild().execute(),
      expandColumn, lowerBound, upperBound, expandDirection,
      getDistinctVertexColumns(getLeftChild().getEmbeddingMetaData()),
      getDistinctEdgeColumns(getLeftChild().getEmbeddingMetaData()),
      closingColumn, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES).evaluate();
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    EmbeddingMetaData inputMetaData = getLeftChild().getEmbeddingMetaData();
    EmbeddingMetaData metaData = new EmbeddingMetaData(inputMetaData);

    metaData.setEntryColumn(pathVariable, EmbeddingMetaData.EntryType.PATH,
      inputMetaData.getEntryCount());

    if (!inputMetaData.containsEntryColumn(endVariable)) {
      metaData.setEntryColumn(endVariable, EmbeddingMetaData.EntryType.VERTEX,
        inputMetaData.getEntryCount() + 1);
    }
    return metaData;
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#vertexStrategy} and the specified
   * {@link EmbeddingMetaData}, the method returns the columns that need to contain distinct
   * entries.
   *
   * @param metaData meta data for the embedding
   * @return distinct vertex columns
   */
  private List<Integer> getDistinctVertexColumns(EmbeddingMetaData metaData) {
    return this.vertexStrategy == MatchStrategy.ISOMORPHISM ? metaData.getVertexVariables().stream()
      .map(metaData::getEntryColumn)
      .collect(Collectors.toList()) : Collections.emptyList();
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#edgeStrategy} and the specified
   * {@link EmbeddingMetaData}, the method returns the columns that need to contain distinct
   * entries.
   *
   * @param metaData meta data for the embedding
   * @return distinct edge columns
   */
  private List<Integer> getDistinctEdgeColumns(EmbeddingMetaData metaData) {
    return edgeStrategy == MatchStrategy.ISOMORPHISM ?
      metaData.getEdgeVariables().stream()
        .map(metaData::getEntryColumn)
        .collect(Collectors.toList()) : Collections.emptyList();
  }

  @Override
  public String toString() {
    return String.format("ExpandEmbeddingsNode={" +
        "startVariable='%s', " +
        "pathVariable='%s', " +
        "endVariable='%s', " +
        "lowerBound=%d, " +
        "upperBound=%d, " +
        "expandDirection=%s, " +
        "vertexMorphismType=%s, " +
        "edgeMorphismType=%s}",
      startVariable, pathVariable, edgeStrategy, lowerBound, upperBound, expandDirection,
      vertexStrategy, edgeStrategy);
  }
}
