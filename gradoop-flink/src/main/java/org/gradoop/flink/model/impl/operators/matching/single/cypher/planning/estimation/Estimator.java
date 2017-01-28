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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;

/**
 * Base class for estimators that provides some utility methods.
 */
abstract class Estimator {
  /**
   * Query handler to get information about query elements
   */
  private final QueryHandler queryHandler;
  /**
   * Statistics about the search graph
   */
  private final GraphStatistics graphStatistics;

  /**
   * Creates a new estimator.
   *
   * @param queryHandler query handler
   * @param graphStatistics graph statistics
   */
  Estimator(QueryHandler queryHandler, GraphStatistics graphStatistics) {
    this.queryHandler = queryHandler;
    this.graphStatistics = graphStatistics;
  }

  public QueryHandler getQueryHandler() {
    return queryHandler;
  }

  public GraphStatistics getGraphStatistics() {
    return graphStatistics;
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
