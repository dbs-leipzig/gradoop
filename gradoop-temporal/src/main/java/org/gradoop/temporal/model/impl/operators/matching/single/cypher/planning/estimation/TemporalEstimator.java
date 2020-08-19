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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;


import org.gradoop.temporal.model.impl.operators.matching.common.query.TemporalQueryHandler;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;

/**
 * Base class for estimators that provides some utility methods.
 */
public abstract class TemporalEstimator {
  /**
   * Query handler to get information about query elements
   */
  private final TemporalQueryHandler queryHandler;
  /**
   * Statistics about the search graph
   */
  private final TemporalGraphStatistics graphStatistics;

  /**
   * Creates a new estimator.
   *
   * @param queryHandler    query handler
   * @param graphStatistics graph statistics
   */
  TemporalEstimator(TemporalQueryHandler queryHandler, TemporalGraphStatistics graphStatistics) {
    this.queryHandler = queryHandler;
    this.graphStatistics = graphStatistics;
  }

  public TemporalQueryHandler getQueryHandler() {
    return queryHandler;
  }

  public TemporalGraphStatistics getGraphStatistics() {
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
   * @param label    label
   * @param isVertex true, iff label maps to a vertex
   * @return number of elements with the given label
   */
  long getCardinality(String label, boolean isVertex) {
    return isVertex ?
      graphStatistics.getVertexCount(label) :
      graphStatistics.getEdgeCount(label);
  }
}
