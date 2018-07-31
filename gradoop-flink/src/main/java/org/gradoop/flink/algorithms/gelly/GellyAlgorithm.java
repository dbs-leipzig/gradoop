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
package org.gradoop.flink.algorithms.gelly;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdge;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Base class for Algorithms executed in Flink Gelly.
 *
 * @param <E> Value type for vertices.
 * @param <F> Value type for edges.
 */
public abstract class GellyAlgorithm<E, F> implements UnaryGraphToGraphOperator {

  /**
   * The graph used in {@link GellyAlgorithm#execute(LogicalGraph)}.
   */
  protected LogicalGraph currentGraph;

  /**
   * Function mapping to edge to gelly edge.
   */
  private final EdgeToGellyEdge<F> toGellyEdge;

  /**
   * Function mapping vertex to gelly vertex.
   */
  private final VertexToGellyVertex<E> toGellyVertex;

  /**
   * Base constructor, only setting the mapper functions.
   *
   * @param vertexValue Function mapping vertices from Gradoop to Gelly.
   * @param edgeValue   function mapping edges from Gradoop to Gelly.
   */
  protected GellyAlgorithm(VertexToGellyVertex<E> vertexValue, EdgeToGellyEdge<F> edgeValue) {
    this.toGellyVertex = vertexValue;
    this.toGellyEdge = edgeValue;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    currentGraph = graph;
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
  protected Graph<GradoopId, E, F> transformToGelly(LogicalGraph graph) {
    DataSet<Vertex<GradoopId, E>> gellyVertices = graph.getVertices().map(toGellyVertex);
    DataSet<Edge<GradoopId, F>> gellyEdges = graph.getEdges().map(toGellyEdge);
    return Graph.fromDataSet(gellyVertices, gellyEdges,
      graph.getConfig().getExecutionEnvironment());
  }

  /**
   * Perform some operation in Gelly and transform the Gelly graph back to a Gradoop
   * {@link LogicalGraph}.
   *
   * @param graph The Gelly graph.
   * @return The Gradoop graph.
   */
  protected abstract LogicalGraph executeInGelly(Graph<GradoopId, E, F> graph) throws Exception;
}
