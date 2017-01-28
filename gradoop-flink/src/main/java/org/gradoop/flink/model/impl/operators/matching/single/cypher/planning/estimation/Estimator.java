package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;

public abstract class Estimator {
  /**
   * Query handler to get information about query elements
   */
  final QueryHandler queryHandler;
  /**
   * Statistics about the search graph
   */
  final GraphStatistics graphStatistics;

  Estimator(QueryHandler queryHandler, GraphStatistics graphStatistics) {
    this.queryHandler = queryHandler;
    this.graphStatistics = graphStatistics;
  }

  /**
   * Returns the label of the given variable.
   *
   * @param variable query variable
   * @param isVertex true, iff the variable maps to a vertex
   * @return label
   */
  String getLabel(String variable, boolean isVertex) {
    return isVertex ? queryHandler.getVertexByVariable(variable).getLabel() :
      queryHandler.getEdgeByVariable(variable).getLabel();
  }

  /**
   * Returns the cardinality of the specified label according to the provided statistics.
   *
   * @param label label
   * @param isVertex true, iff label maps to a vertex
   * @return number of elements with the given label
   */
  long getCardinality(String label, boolean isVertex) {
    long cardinality = isVertex ? graphStatistics.getVertexCountByLabel(label) :
      graphStatistics.getEdgeCountByLabel(label);

    return cardinality > 0 ? cardinality :
      isVertex ? graphStatistics.getVertexCount() : graphStatistics.getEdgeCount();
  }
}
