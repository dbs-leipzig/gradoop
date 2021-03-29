/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.PropertyRemover;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingConstants;

/**
 * Retains all vertices which do not have the given degree.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class FilterVerticesWithDegreeOtherThanGiven<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

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

  @Override
  public LG execute(LG graph) {

    DistinctVertexDegrees<G, V, E, LG, GC> distinctVertexDegrees = new DistinctVertexDegrees<>(
      SamplingConstants.DEGREE_PROPERTY_KEY,
      SamplingConstants.IN_DEGREE_PROPERTY_KEY,
      SamplingConstants.OUT_DEGREE_PROPERTY_KEY,
      true);

    DataSet<V> newVertices = distinctVertexDegrees.execute(graph).getVertices()
      .filter(new VertexWithDegreeFilter<>(degree, SamplingConstants.DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingConstants.DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingConstants.IN_DEGREE_PROPERTY_KEY))
      .map(new PropertyRemover<>(SamplingConstants.OUT_DEGREE_PROPERTY_KEY));

    return graph.getFactory().fromDataSets(
      graph.getGraphHead(), newVertices, graph.getEdges());
  }
}
