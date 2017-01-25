package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

import java.util.Set;

public class JoinEmbeddingsEstimator implements Estimator {

  private final GraphStatistics graphStatistics;
  private final Set<String> joinVariables;
  private final long leftCardinality;
  private final long rightCardinality;
  private final EmbeddingMetaData leftInputMetaData;
  private final EmbeddingMetaData rightInputMetaData;

  public JoinEmbeddingsEstimator(GraphStatistics graphStatistics, Set<String> joinVariables,
    long leftCardinality, long rightCardinality,
    EmbeddingMetaData leftInputMetaData, EmbeddingMetaData rightInputMetaData) {
    this.graphStatistics = graphStatistics;
    this.joinVariables = joinVariables;
    this.leftCardinality = leftCardinality;
    this.rightCardinality = rightCardinality;
    this.leftInputMetaData = leftInputMetaData;
    this.rightInputMetaData = rightInputMetaData;
  }

  @Override
  public long getCardinality() {
    return leftCardinality * rightCardinality;
  }
}
