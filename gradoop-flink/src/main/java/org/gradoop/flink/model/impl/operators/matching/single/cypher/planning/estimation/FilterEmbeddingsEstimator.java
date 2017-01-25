package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

import java.util.Set;

public class FilterEmbeddingsEstimator implements Estimator {

  private final GraphStatistics graphStatistics;

  private final CNF predicate;

  private final long inputCardinality;

  private final EmbeddingMetaData inputMetaData;

  public FilterEmbeddingsEstimator(GraphStatistics graphStatistics, Set<String> variables,
    CNF predicate, long inputCardinality, EmbeddingMetaData inputMetaData) {
    this.graphStatistics = graphStatistics;
    this.predicate = predicate;
    this.inputCardinality = inputCardinality;
    this.inputMetaData = inputMetaData;
  }

  @Override
  public long getCardinality() {
    return inputCardinality;
  }
}