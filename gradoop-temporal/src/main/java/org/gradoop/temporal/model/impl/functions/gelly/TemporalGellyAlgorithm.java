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
package org.gradoop.temporal.model.impl.functions.gelly;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.functions.gelly.functions.TemporalEdgeToGellyEdge;
import org.gradoop.temporal.model.impl.functions.gelly.functions.TemporalVertexToGellyVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

/**
 *  Base class for Algorithms executed in Flink Gelly that returns a {@link TemporalGraph}.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 * @param <VV> Value type for gelly vertices.
 * @param <EV> Value type for gelly edges.
 */

public abstract class TemporalGellyAlgorithm<
        G extends TemporalGraphHead,
        V extends TemporalVertex,
        E extends TemporalEdge,
        LG extends TemporalGraph,
        GC extends TemporalGraphCollection,
        VV, EV> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * The graph used in {@link #execute}.
   */
  protected TemporalGraph currentGraph;

  /**
   * Function mapping temporal edge to gelly edge.
   */
  private final TemporalEdgeToGellyEdge<TemporalEdge, EV> toGellyEdge;

  /**
   * Function mapping temporal vertex to gelly vertex.
   */
  private final TemporalVertexToGellyVertex<TemporalVertex, VV> toGellyVertex;

  /**
   * Base constructor, only setting the mapper functions.
   *
   * @param toGellyVertex Function mapping temporal vertices from Gradoop to Gelly.
   * @param toGellyEdge   function mapping temporal edges from Gradoop to Gelly.
   */
  public TemporalGellyAlgorithm(TemporalEdgeToGellyEdge<TemporalEdge, EV> toGellyEdge,
                                  TemporalVertexToGellyVertex<TemporalVertex, VV> toGellyVertex) {
    this.toGellyEdge = toGellyEdge;
    this.toGellyVertex = toGellyVertex;
  }

    /**
     * Execution of algorithm
     * @param graph input graph
     * @return Temporal Gradoop Graph
     */
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
   * @param graph Temporal Gradoop Graph.
   * @return Gelly Graph.
   */
  public Graph<GradoopId, VV, EV> transformToGelly(LG graph) {
    DataSet<org.apache.flink.graph.Vertex<GradoopId, VV>>
            gellyVertices = graph.getVertices().map(toGellyVertex);
    DataSet<org.apache.flink.graph.Edge<GradoopId, EV>> gellyEdges = graph.getEdges().map(toGellyEdge);

    return Graph.fromDataSet(gellyVertices, gellyEdges, graph.getConfig().getExecutionEnvironment());
  }


  /**
   * Perform some operation in Gelly and transform the Gelly graph back to a Gradoop {@link TemporalGraph}.
   *
   * @param gellyGraph The Gelly graph.
   * @return The Temporal Gradoop graph.
   * @throws Exception on failure
   */
  public abstract LG executeInGelly(Graph<GradoopId, VV, EV> gellyGraph) throws Exception;
}
