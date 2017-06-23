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

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.CartesianProduct;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps a {@link CartesianProduct} operator.
 */
public class CartesianProductNode extends BinaryNode implements JoinNode {
  /**
   * Morphism type for vertices
   */
  private final MatchStrategy vertexStrategy;
  /**
   * Morphism type for edges
   */
  private final MatchStrategy edgeStrategy;
  /**
   * Cross hint for Flink optimizer
   */
  private final CrossOperatorBase.CrossHint crossHint;

  /**
   * Creates  a new node.
   *
   * @param leftChild left input plan node
   * @param rightChild right right input plan node
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   */
  public CartesianProductNode(PlanNode leftChild, PlanNode rightChild,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this(leftChild, rightChild,
      vertexStrategy, edgeStrategy, CrossOperatorBase.CrossHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Creates a new node.
   *
   * @param leftChild left input plan node
   * @param rightChild right input plan node
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   * @param crossHint Join hint for the Flink optimizer
   */
  public CartesianProductNode(PlanNode leftChild, PlanNode rightChild,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    CrossOperatorBase.CrossHint crossHint) {
    super(leftChild, rightChild);
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.crossHint = crossHint;
  }

  @Override
  public DataSet<Embedding> execute() {
    CartesianProduct op = new CartesianProduct(getLeftChild().execute(), getRightChild().execute(),
      getRightChild().getEmbeddingMetaData().getEntryCount(),
      getDistinctVertexColumnsLeft(), getDistinctVertexColumnsRight(),
      getDistinctEdgeColumnsLeft(), getDistinctEdgeColumnsRight(), crossHint);
    op.setName(this.toString());
    return op.evaluate();
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    EmbeddingMetaData leftInputMetaData = getLeftChild().getEmbeddingMetaData();
    EmbeddingMetaData rightInputMetaData = getRightChild().getEmbeddingMetaData();
    EmbeddingMetaData embeddingMetaData = new EmbeddingMetaData(leftInputMetaData);

    int entryCount = leftInputMetaData.getEntryCount();

    // append the non-join entry mappings from the right to the left side
    for (String var : rightInputMetaData.getVariables()) {
      embeddingMetaData.setEntryColumn(var, rightInputMetaData.getEntryType(var), entryCount++);
    }

    // append all property mappings from the right to the left side
    int propertyCount = leftInputMetaData.getPropertyCount();
    for (String var : rightInputMetaData.getVariables()) {
      for (String key : rightInputMetaData.getPropertyKeys(var)) {
        embeddingMetaData.setPropertyColumn(var, key, propertyCount++);
      }
    }
    return embeddingMetaData;
  }

  /**
   * According to the specified {@link CartesianProductNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   *
   * @return distinct vertex columns of the left embedding
   */
  private List<Integer> getDistinctVertexColumnsLeft() {
    return getDistinctVertexColumns(getLeftChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link CartesianProductNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   *
   * @return distinct vertex columns of the right embedding
   */
  private List<Integer> getDistinctVertexColumnsRight() {
    return getDistinctVertexColumns(getRightChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link CartesianProductNode#vertexStrategy} and the specified
   * {@link EmbeddingMetaData}, the method returns the columns that need to contain distinct
   * entries.
   *
   * @param metaData meta data for the embedding
   * @return distinct vertex columns
   */
  private List<Integer> getDistinctVertexColumns(final EmbeddingMetaData metaData) {
    return vertexStrategy == MatchStrategy.ISOMORPHISM ?
      metaData.getVertexVariables().stream()
        .map(metaData::getEntryColumn)
        .collect(Collectors.toList()) : Collections.emptyList();
  }

  /**
   * According to the specified {@link CartesianProductNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   *
   * @return distinct edge columns of the left embedding
   */
  private List<Integer> getDistinctEdgeColumnsLeft() {
    return getDistinctEdgeColumns(getLeftChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link CartesianProductNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   *
   * @return distinct edge columns of the right embedding
   */
  private List<Integer> getDistinctEdgeColumnsRight() {
    return getDistinctEdgeColumns(getRightChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link CartesianProductNode#edgeStrategy} and the specified
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
    return String.format("CartesianProductNode{" +
      "vertexMorphismType=%s, " +
      "edgeMorphismType=%s}",
      vertexStrategy, edgeStrategy);
  }
}
