package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.JoinEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.BinaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Binary node that wraps a {@link JoinEmbeddings} operator.
 */
public class JoinEmbeddingsNode implements BinaryNode {

  private final PlanNode leftChild;

  private final PlanNode rightChild;

  private final List<Integer> joinColumnsLeft;

  private final List<Integer> joinColumnsRight;

  private final MatchStrategy vertexStrategy;

  private final MatchStrategy edgeStrategy;

  public JoinEmbeddingsNode(PlanNode leftChild, PlanNode rightChild,
    List<String> joinVariablesLeft, List<String> joinVariablesRight,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;

    joinColumnsLeft = joinVariablesLeft.stream()
      .map(var -> leftChild.getEmbeddingMetaData().getEntryColumn(var))
      .collect(Collectors.toList());

    joinColumnsRight = joinVariablesRight.stream()
      .map(var -> leftChild.getEmbeddingMetaData().getEntryColumn(var))
      .collect(Collectors.toList());
  }

  @Override
  public PlanNode getLeftChild() {
    return leftChild;
  }

  @Override
  public PlanNode getRightChild() {
    return rightChild;
  }

  @Override
  public DataSet<Embedding> execute() {
    return new JoinEmbeddings(leftChild.execute(), rightChild.execute(),
      joinColumnsLeft, joinColumnsRight).evaluate();
  }

  @Override
  public Estimator getEstimator() {
    return null;
  }

  @Override
  public EmbeddingMetaData getEmbeddingMetaData() {
    return null;
  }
}
