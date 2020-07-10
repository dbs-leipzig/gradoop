/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.binary;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.JoinNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.ValueJoin;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.BinaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Set;
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
   * left time data used for the join
   */
  private final List<Pair<String, String>> leftJoinTimeData;
  /**
   * right time data used for the join
   */
  private final List<Pair<String, String>> rightJoinTimeData;
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
   * @param leftChild           left input plan node
   * @param rightChild          right right input plan node
   * @param leftJoinProperties  properties used for joining on the left side
   * @param rightJoinProperties properties used for joining on the right side
   * @param leftJoinTimeData    time data used for joining on the left side
   * @param rightJoinTimeData   time data used for joining on the right side
   * @param vertexStrategy      morphism setting for vertices
   * @param edgeStrategy        morphism setting for edges
   */
  public ValueJoinNode(PlanNode leftChild, PlanNode rightChild,
                       List<Pair<String, String>> leftJoinProperties,
                       List<Pair<String, String>> rightJoinProperties,
                       List<Pair<String, String>> leftJoinTimeData,
                       List<Pair<String, String>> rightJoinTimeData,
                       MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this(leftChild, rightChild, leftJoinProperties, rightJoinProperties, leftJoinTimeData,
      rightJoinTimeData, vertexStrategy, edgeStrategy, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES);
  }

  /**
   * Creates a new node.
   *
   * @param leftChild           left input plan node
   * @param rightChild          right input plan node
   * @param leftJoinProperties  properties used for joining on the left side
   * @param rightJoinProperties properties used for joining on the right side
   * @param leftJoinTimeData    time data used for joining on the left side
   * @param rightJoinTimeData   time data used for joining on the right side
   * @param vertexStrategy      morphism setting for vertices
   * @param edgeStrategy        morphism setting for edges
   * @param joinHint            Join hint for the Flink optimizer
   */
  public ValueJoinNode(PlanNode leftChild, PlanNode rightChild,
                       List<Pair<String, String>> leftJoinProperties,
                       List<Pair<String, String>> rightJoinProperties,
                       List<Pair<String, String>> leftJoinTimeData,
                       List<Pair<String, String>> rightJoinTimeData,
                       MatchStrategy vertexStrategy, MatchStrategy edgeStrategy,
                       JoinOperatorBase.JoinHint joinHint) {
    super(leftChild, rightChild);
    this.leftJoinProperties = leftJoinProperties;
    this.rightJoinProperties = rightJoinProperties;
    this.leftJoinTimeData = leftJoinTimeData;
    this.rightJoinTimeData = rightJoinTimeData;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
    this.joinHint = joinHint;
  }

  @Override
  public DataSet<EmbeddingTPGM> execute() {
    ValueJoin op = new ValueJoin(getLeftChild().execute(), getRightChild().execute(),
      getJoinPropertiesLeft(), getJoinPropertiesRight(),
      getTimeDataLeft(), getTimeDataRight(),
      getRightChild().getEmbeddingMetaData().getEntryCount(),
      getDistinctVertexColumnsLeft(), getDistinctVertexColumnsRight(),
      getDistinctEdgeColumnsLeft(), getDistinctEdgeColumnsRight(),
      joinHint);
    op.setName(this.toString());
    return op.evaluate();
  }

  @Override
  protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
    EmbeddingTPGMMetaData leftInputMetaData = getLeftChild().getEmbeddingMetaData();
    EmbeddingTPGMMetaData rightInputMetaData = getRightChild().getEmbeddingMetaData();
    EmbeddingTPGMMetaData embeddingMetaData = new EmbeddingTPGMMetaData(leftInputMetaData);

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

    // append all time data from the right to the left side
    int timeCount = leftInputMetaData.getTimeCount();
    Set<String> rightTimeVariables = rightInputMetaData.getTimeDataMapping().keySet();
    // not possible to iterate over rightTimeVariables, sequence of variables is important
    for (String var : rightInputMetaData.getVariables()) {
      if (!rightTimeVariables.contains(var)) {
        continue;
      }
      embeddingMetaData.setTimeColumn(var, timeCount++);
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
   * Computes the column indices and their entry of the left embeddings join time data according to its
   * associated meta data.
   *
   * @return List of tuples of join time data columns and their entry of the left embedding
   */
  private List<Tuple2<Integer, Integer>> getTimeDataLeft() {
    return leftJoinTimeData.stream()
      .map(t -> new Tuple2<>(getLeftChild().getEmbeddingMetaData().getTimeColumn(t.getKey()),
        parseTimeSelector(t.getValue())))
      .collect(Collectors.toList());
  }

  /**
   * Computes the column indices and their entry of the right embeddings join time data according to its
   * associated meta data.
   *
   * @return List of tuples of join time data columns and their entry of the right embedding
   */
  private List<Tuple2<Integer, Integer>> getTimeDataRight() {
    return rightJoinTimeData.stream()
      .map(t -> new Tuple2<>(getRightChild().getEmbeddingMetaData().getTimeColumn(t.getKey()),
        parseTimeSelector(t.getValue())))
      .collect(Collectors.toList());
  }

  /**
   * Maps a time selector string (tx_from, tx_to, valid_from, valid_to) to its position within
   * a time data embedding column
   *
   * @param timeSelector the time selector string
   * @return its position within a time data embedding column
   */
  private int parseTimeSelector(String timeSelector) {
    timeSelector = timeSelector.trim().toLowerCase();
    if (timeSelector.equals("tx_from")) {
      return 0;
    }
    if (timeSelector.equals("tx_to")) {
      return 1;
    }
    if (timeSelector.equals("val_from") || timeSelector.equals("valid_from")) {
      return 2;
    }
    if (timeSelector.equals("val_to") || timeSelector.equals("valid_to")) {
      return 3;
    }
    return 0;
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
   * {@link EmbeddingTPGMMetaData}, the method returns the columns that need to contain distinct
   * entries.
   *
   * @param metaData meta data for the embedding
   * @return distinct vertex columns
   */
  private List<Integer> getDistinctVertexColumns(final EmbeddingTPGMMetaData metaData) {
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
   * {@link EmbeddingTPGMMetaData}, the method returns the columns that need to contain distinct
   * entries.
   *
   * @param metaData meta data for the embedding
   * @return distinct edge columns
   */
  private List<Integer> getDistinctEdgeColumns(EmbeddingTPGMMetaData metaData) {
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
