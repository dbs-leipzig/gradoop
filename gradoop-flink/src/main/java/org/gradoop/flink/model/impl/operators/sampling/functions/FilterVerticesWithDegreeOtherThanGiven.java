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

/**
 * Retains all vertices which do not have the given degree.
 */
public class FilterVerticesWithDegreeOtherThanGiven implements UnaryGraphToGraphOperator {

  /**
   * Key of degree property
   */
  private static final String DEGREE_PROPERTY_KEY = VertexDegree.BOTH.getName();

  /**
   * Key of in-degree property
   */
  private static final String IN_DEGREE_PROPERTY_KEY = VertexDegree.IN.getName();

  /**
   * Key of out-degree property
   */
  private static final String OUT_DEGREE_PROPERTY_KEY = VertexDegree.OUT.getName();

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
      DEGREE_PROPERTY_KEY,
      IN_DEGREE_PROPERTY_KEY,
      OUT_DEGREE_PROPERTY_KEY,
      true);
    DataSet<Vertex> newVertices = distinctVertexDegrees.execute(graph).getVertices();

    newVertices = newVertices.filter(new VertexWithDegreeFilter<>(degree, DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(IN_DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(OUT_DEGREE_PROPERTY_KEY));

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
