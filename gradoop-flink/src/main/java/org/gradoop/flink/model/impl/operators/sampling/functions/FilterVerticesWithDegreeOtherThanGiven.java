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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.PropertyRemover;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;

/**
 * Retains all vertices which do not have the given degree.
 */
public class FilterVerticesWithDegreeOtherThanGiven implements UnaryGraphToGraphOperator {

  /**
   * the given degree
   */
  private long degree;

  /**
   * Constructor
   *
   * @param degree the given degree
   */
  public FilterVerticesWithDegreeOtherThanGiven(long degree) {
    this.degree = degree;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DistinctVertexDegrees distinctVertexDegrees = new DistinctVertexDegrees(
      SamplingAlgorithm.DEGREE_PROPERTY_KEY,
      SamplingAlgorithm.IN_DEGREE_PROPERTY_KEY,
      SamplingAlgorithm.OUT_DEGREE_PROPERTY_KEY,
      true);

    DataSet<Vertex> newVertices = distinctVertexDegrees.execute(graph).getVertices()
      .filter(new VertexWithDegreeFilter<>(degree, SamplingAlgorithm.DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingAlgorithm.DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingAlgorithm.IN_DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingAlgorithm.OUT_DEGREE_PROPERTY_KEY));

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graph.getGraphHead(), newVertices, graph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return FilterVerticesWithDegreeOtherThanGiven.class.getName();
  }
}
