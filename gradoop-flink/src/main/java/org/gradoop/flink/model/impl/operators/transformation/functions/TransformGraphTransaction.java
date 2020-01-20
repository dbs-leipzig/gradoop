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
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.stream.Collectors;

/**
 * Transformation map function for graph transactions.
 *
 * Since {@link GraphTransaction} is only implemented for EPGM, this can only be used with EPGM graphs.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public class TransformGraphTransaction<G extends GraphHead, V extends Vertex, E extends Edge>
  implements MapFunction<GraphTransaction, GraphTransaction> {

  /**
   * Factory to init modified graph head.
   */
  private final GraphHeadFactory<G> graphHeadFactory;
  /**
   * Graph head modification function
   */
  private final TransformationFunction<G> graphHeadTransFunc;

  /**
   * Factory to init modified vertex.
   */
  private final VertexFactory<V> vertexFactory;

  /**
   * EPGMVertex modification function
   */
  private final TransformationFunction<V> vertexTransFunc;

  /**
   * Factory to init modified edge.
   */
  private final EdgeFactory<E> edgeFactory;
  /**
   * EPGMEdge modification function
   */
  private final TransformationFunction<E> edgeTransFunc;

  /**
   * Constructor
   *
   * @param graphHeadFactory graph head factory
   * @param graphHeadTransFunc graph head transformation function
   * @param vertexFactory vertex factory
   * @param vertexTransFunc vertex transformation function
   * @param edgeFactory edge factory
   * @param edgeTransFunc edge transformation function
   */
  public TransformGraphTransaction(GraphHeadFactory<G> graphHeadFactory,
    TransformationFunction<G> graphHeadTransFunc, VertexFactory<V> vertexFactory,
    TransformationFunction<V> vertexTransFunc, EdgeFactory<E> edgeFactory,
    TransformationFunction<E> edgeTransFunc) {
    if (EPGMGraphHead.class.isAssignableFrom(graphHeadFactory.getType()) &&
      EPGMVertex.class.isAssignableFrom(vertexFactory.getType()) &&
      EPGMEdge.class.isAssignableFrom(edgeFactory.getType())) {
      this.graphHeadFactory = graphHeadFactory;
      this.graphHeadTransFunc = graphHeadTransFunc;
      this.vertexFactory = vertexFactory;
      this.vertexTransFunc = vertexTransFunc;
      this.edgeFactory = edgeFactory;
      this.edgeTransFunc = edgeTransFunc;
    } else {
      throw new UnsupportedOperationException("This map function only supports EPGM graphs");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public GraphTransaction map(GraphTransaction transaction) throws Exception {
    if (graphHeadTransFunc != null) {
      transaction.setGraphHead((EPGMGraphHead) graphHeadTransFunc.apply(
        (G) transaction.getGraphHead(),
        graphHeadFactory.initGraphHead(
          transaction.getGraphHead().getId(), GradoopConstants.DEFAULT_GRAPH_LABEL)));
    }

    if (vertexTransFunc != null) {
      transaction.setVertices(transaction.getVertices().stream()
        .map(vertex -> (EPGMVertex) vertexTransFunc.apply(
          (V) vertex,
          vertexFactory.initVertex(
            vertex.getId(), GradoopConstants.DEFAULT_VERTEX_LABEL, vertex.getGraphIds())))
        .collect(Collectors.toSet()));
    }

    if (edgeTransFunc != null) {
      transaction.setEdges(transaction.getEdges().stream()
        .map(edge -> (EPGMEdge) edgeTransFunc.apply(
          (E) edge,
          edgeFactory.initEdge(
            edge.getId(), GradoopConstants.DEFAULT_EDGE_LABEL,
            edge.getSourceId(), edge.getTargetId(), edge.getGraphIds())))
        .collect(Collectors.toSet()));
    }
    return transaction;
  }
}
