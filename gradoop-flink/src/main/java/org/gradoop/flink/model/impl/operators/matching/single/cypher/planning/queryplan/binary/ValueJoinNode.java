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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.ValueJoin;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps a {@link ValueJoin} operator.
 */
public class ValueJoinNode extends BinaryNode implements JoinNode {
  /**
   * Vertex and Edge properties of the left side that are used for the join
   */
  private final List<Pair<String, String>> leftJoinProperties;
  /**
   * Vertex and Edge properties of the right side that are used for the join
   */
  private final List<Pair<String, String>> rightJoinProperties;
  /**
   * Morphism type for vertices
   */
  private final MatchStrategy vertexStrategy;
  /**
   * Morphism type for edges
   */
  private final MatchStrategy edgeStrategy;
  /**
   * Join hint for Flink optimizer
   */
  private final JoinOperatorBase.JoinHint joinHint;

  /**
   * Creates  a new node.
   *
   * @param leftChild left input plan node
   * @param rightChild right right input plan node
   * @param leftJoinProperties properties used for joining on the left side
   * @param rightJoinProperties properties used for joining on the right side
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   */
  public ValueJoinNode(PlanNode leftChild, PlanNode rightChild,
    List<Pair<String, String>> leftJoinProperties, List<Pair<String, String>> rightJoinProperties,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this(leftChild, rightChild, leftJoinProperties, rightJoinProperties,
      vertexStrategy, edgeStrategy, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Creates a new node.
   *
   * @param leftChild left input plan node
   * @param rightChild right input plan node
   * @param leftJoinProperties properties used for joining on the left side
   * @param rightJoinProperties properties used for joining on the right side
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   * @param joinHint Join hint for the Flink optimizer
   */
  public ValueJoinNode(PlanNode leftChild, PlanNode rightChild,
    List<Pair<String, String>> leftJoinProperties, List<Pair<String, String>> rightJoinProperties,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    JoinOperatorBase.JoinHint joinHint) {
    super(leftChild, rightChild);
    this.leftJoinProperties = leftJoinProperties;
    this.rightJoinProperties = rightJoinProperties;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.joinHint = joinHint;
  }

  @Override
  public DataSet<Embedding> execute() {
    ValueJoin op = new ValueJoin(getLeftChild().execute(), getRightChild().execute(),
      getJoinPropertiesLeft(), getJoinPropertiesRight(),
      getRightChild().getEmbeddingMetaData().getEntryCount(),
      getDistinctVertexColumnsLeft(), getDistinctVertexColumnsRight(),
      getDistinctEdgeColumnsLeft(), getDistinctEdgeColumnsRight(),
      joinHint);
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
   * Computes the column indices of the left embeddings join properties according to its
   * associated meta data.
   *
   * @return join property columns of the left embedding
   */
  private List<Integer> getJoinPropertiesLeft() {
    return leftJoinProperties.stream()
      .map(p -> getLeftChild().getEmbeddingMetaData().getPropertyColumn(p.getKey(), p.getValue()))
      .collect(Collectors.toList());
  }

  /**
   * Computes the column indices of the right embeddings join properties according to its
   * associated meta data.
   *
   * @return join property columns of the right embedding
   */
  private List<Integer> getJoinPropertiesRight() {
    return rightJoinProperties.stream()
      .map(p -> getRightChild().getEmbeddingMetaData().getPropertyColumn(p.getKey(), p.getValue()))
      .collect(Collectors.toList());
  }

  /**
   * According to the specified {@link ValueJoinNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   *
   * @return distinct vertex columns of the left embedding
   */
  private List<Integer> getDistinctVertexColumnsLeft() {
    return getDistinctVertexColumns(getLeftChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link ValueJoinNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   *
   * @return distinct vertex columns of the right embedding
   */
  private List<Integer> getDistinctVertexColumnsRight() {
    return getDistinctVertexColumns(getRightChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link ValueJoinNode#vertexStrategy} and the specified
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
   * According to the specified {@link ValueJoinNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   *
   * @return distinct edge columns of the left embedding
   */
  private List<Integer> getDistinctEdgeColumnsLeft() {
    return getDistinctEdgeColumns(getLeftChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link ValueJoinNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   *
   * @return distinct edge columns of the right embedding
   */
  private List<Integer> getDistinctEdgeColumnsRight() {
    return getDistinctEdgeColumns(getRightChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link ValueJoinNode#edgeStrategy} and the specified
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
    return String.format("ValueJoinNode{" +
      "leftJoinProperties=%s, " +
      "rightJoinProperties=%s, " +
      "vertexMorphismType=%s, " +
      "edgeMorphismType=%s}",
      leftJoinProperties, rightJoinProperties, vertexStrategy, edgeStrategy);
  }
}
