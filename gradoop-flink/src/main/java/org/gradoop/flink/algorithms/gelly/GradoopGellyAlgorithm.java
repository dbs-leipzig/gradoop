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
package org.gradoop.flink.algorithms.gelly;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdge;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;

/**
 * Base class for Algorithms executed in Flink Gelly that returns a {@link BaseGraph}.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 * @param <VV> Value type for gelly vertices.
 * @param <EV> Value type for gelly edges.
 */
public abstract class GradoopGellyAlgorithm<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>,
  VV, EV> extends BaseGellyAlgorithm<G, V, E, LG, GC, GradoopId, VV, EV, LG>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * The graph used in {@link #execute}.
   */
  protected BaseGraph<G, V, E, LG, GC> currentGraph;

  /**
   * Function mapping edge to gelly edge.
   */
  private final EdgeToGellyEdge<E, EV> toGellyEdge;

  /**
   * Function mapping vertex to gelly vertex.
   */
  private final VertexToGellyVertex<V, VV> toGellyVertex;

  /**
   * Base constructor, only setting the mapper functions.
   *
   * @param vertexValue Function mapping vertices from Gradoop to Gelly.
   * @param edgeValue   function mapping edges from Gradoop to Gelly.
   */
  protected GradoopGellyAlgorithm(
    VertexToGellyVertex<V, VV> vertexValue, EdgeToGellyEdge<E, EV> edgeValue) {
    this.toGellyVertex = vertexValue;
    this.toGellyEdge = edgeValue;
  }

  @Override
  public LG execute(LG graph) {
    this.currentGraph = graph;
    try {
      return executeInGelly(transformToGelly(graph));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Default transformation from a Gradoop Graph to a Gelly Graph.
   *
   * @param graph Gradoop Graph.
   * @return Gelly Graph.
   */
  public Graph<GradoopId, VV, EV> transformToGelly(LG graph) {
    DataSet<org.apache.flink.graph.Vertex<GradoopId, VV>> gellyVertices = graph.getVertices()
      .map(toGellyVertex);
    DataSet<org.apache.flink.graph.Edge<GradoopId, EV>> gellyEdges = graph.getEdges().map(toGellyEdge);
    return Graph.fromDataSet(gellyVertices, gellyEdges, graph.getConfig().getExecutionEnvironment());
  }

  /**
   * Perform some operation in Gelly and transform the Gelly graph back to a Gradoop {@link BaseGraph}.
   *
   * @param gellyGraph The Gelly graph.
   * @return The Gradoop graph.
   * @throws Exception on failure
   */
  public abstract LG executeInGelly(Graph<GradoopId, VV, EV> gellyGraph) throws Exception;
}
