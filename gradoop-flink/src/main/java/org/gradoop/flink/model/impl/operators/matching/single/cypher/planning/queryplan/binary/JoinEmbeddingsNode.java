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

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.JoinEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps a {@link JoinEmbeddings} operator.
 */
public class JoinEmbeddingsNode extends BinaryNode implements JoinNode {
  /**
   * Query variables on which left and right child are joined
   */
  private final List<String> joinVariables;
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
   * @param joinVariables query variables to join the inputs on
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   */
  public JoinEmbeddingsNode(PlanNode leftChild, PlanNode rightChild,
    List<String> joinVariables,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this(leftChild, rightChild, joinVariables, vertexStrategy, edgeStrategy,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Creates a new node.
   *
   * @param leftChild left input plan node
   * @param rightChild right input plan node
   * @param joinVariables query variables to join the inputs on
   * @param vertexStrategy morphism setting for vertices
   * @param edgeStrategy morphism setting for edges
   * @param joinHint Join hint for the Flink optimizer
   */
  public JoinEmbeddingsNode(PlanNode leftChild, PlanNode rightChild,
    List<String> joinVariables,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
    JoinOperatorBase.JoinHint joinHint) {
    super(leftChild, rightChild);
    this.joinVariables = joinVariables;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.joinHint = joinHint;
  }

  @Override
  public DataSet<Embedding> execute() {
    JoinEmbeddings op = new JoinEmbeddings(getLeftChild().execute(), getRightChild().execute(),
      getRightChild().getEmbeddingMetaData().getEntryCount(),
      getJoinColumnsLeft(), getJoinColumnsRight(),
      getDistinctVertexColumnsLeft(), getDistinctVertexColumnsRight(),
      getDistinctEdgeColumnsLeft(), getDistinctEdgeColumnsRight(),
      joinHint);
    op.setName(toString());
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
      if (!joinVariables.contains(var)) {
        embeddingMetaData.setEntryColumn(var, rightInputMetaData.getEntryType(var), entryCount++);
      }
      // copy the direction information from the right to the left side
      if (rightInputMetaData.getEntryType(var) == EmbeddingMetaData.EntryType.PATH) {
        embeddingMetaData.setDirection(var, rightInputMetaData.getDirection(var));
      }
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
   * Computes the join columns of the left embedding according to its associated meta data.
   *
   * @return join columns of the left embedding
   */
  private List<Integer> getJoinColumnsLeft() {
    return joinVariables.stream()
      .map(var -> getLeftChild().getEmbeddingMetaData().getEntryColumn(var))
      .collect(Collectors.toList());
  }

  /**
   * Computes the join columns of the right embedding according to its associated meta data.
   *
   * @return join columns of the right embedding
   */
  private List<Integer> getJoinColumnsRight() {
    return joinVariables.stream()
      .map(var -> getRightChild().getEmbeddingMetaData().getEntryColumn(var))
      .collect(Collectors.toList());
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   * This includes the join columns
   *
   * @return distinct vertex columns of the left embedding
   */
  private List<Integer> getDistinctVertexColumnsLeft() {
    EmbeddingMetaData metaData = getLeftChild().getEmbeddingMetaData();

    return vertexStrategy == MatchStrategy.ISOMORPHISM ?
      metaData.getVertexVariables().stream()
        .map(metaData::getEntryColumn)
        .collect(Collectors.toList()) : Collections.emptyList();
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#vertexStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   * This excludes the join columns
   *
   * @return distinct vertex columns of the right embedding
   */
  private List<Integer> getDistinctVertexColumnsRight() {
    EmbeddingMetaData metaData = getRightChild().getEmbeddingMetaData();
    return vertexStrategy == MatchStrategy.ISOMORPHISM ?
      metaData.getVertexVariables().stream()
        .filter(var -> !joinVariables.contains(var))
        .map(metaData::getEntryColumn)
        .collect(Collectors.toList()) : Collections.emptyList();
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the left embedding.
   *
   * @return distinct edge columns of the left embedding
   */
  private List<Integer> getDistinctEdgeColumnsLeft() {
    return getDistinctEdgeColumns(getLeftChild().getEmbeddingMetaData());
  }

  /**
   * According to the specified {@link JoinEmbeddingsNode#edgeStrategy}, the method returns
   * the columns that need to contain distinct entries in the right embedding.
   *
   * @return distinct edge columns of the right embedding
   */
  private List<Integer> getDistinctEdgeColumnsRight() {
    return getDistinctEdgeColumns(getRightChild().getEmbeddingMetaData());
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
    return String.format("JoinEmbeddingsNode{" +
      "joinVariables=%s, " +
      "vertexMorphismType=%s, " +
      "edgeMorphismType=%s}",
      joinVariables, vertexStrategy, edgeStrategy);
  }
}
