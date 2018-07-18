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
package org.gradoop.flink.model.impl.operators.transformation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.stream.Collectors;

/**
 * Transformation map function for graph transactions.
 */
public class TransformGraphTransaction implements MapFunction<GraphTransaction, GraphTransaction> {

  /**
   * Factory to init modified graph head.
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;
  /**
   * Graph head modification function
   */
  private final TransformationFunction<GraphHead> graphHeadTransFunc;

  /**
   * Factory to init modified vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Vertex modification function
   */
  private final TransformationFunction<Vertex> vertexTransFunc;

  /**
   * Factory to init modified edge.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;
  /**
   * Edge modification function
   */
  private final TransformationFunction<Edge> edgeTransFunc;

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
  public TransformGraphTransaction(EPGMGraphHeadFactory<GraphHead> graphHeadFactory,
    TransformationFunction<GraphHead> graphHeadTransFunc, EPGMVertexFactory<Vertex> vertexFactory,
    TransformationFunction<Vertex> vertexTransFunc, EPGMEdgeFactory<Edge> edgeFactory,
    TransformationFunction<Edge> edgeTransFunc) {
    this.graphHeadFactory = graphHeadFactory;
    this.graphHeadTransFunc = graphHeadTransFunc;
    this.vertexFactory = vertexFactory;
    this.vertexTransFunc = vertexTransFunc;
    this.edgeFactory = edgeFactory;
    this.edgeTransFunc = edgeTransFunc;
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {
    if (graphHeadTransFunc != null) {
      transaction.setGraphHead(graphHeadTransFunc.apply(
        transaction.getGraphHead(),
        graphHeadFactory.initGraphHead(
          transaction.getGraphHead().getId(), GradoopConstants.DEFAULT_GRAPH_LABEL)));
    }

    if (vertexTransFunc != null) {
      transaction.setVertices(transaction.getVertices().stream()
        .map(vertex -> vertexTransFunc.apply(
          vertex,
          vertexFactory.initVertex(
            vertex.getId(), GradoopConstants.DEFAULT_VERTEX_LABEL, vertex.getGraphIds())))
        .collect(Collectors.toSet()));
    }

    if (edgeTransFunc != null) {
      transaction.setEdges(transaction.getEdges().stream()
        .map(edge -> edgeTransFunc.apply(
          edge,
          edgeFactory.initEdge(
            edge.getId(), GradoopConstants.DEFAULT_EDGE_LABEL,
            edge.getSourceId(), edge.getTargetId(), edge.getGraphIds())))
        .collect(Collectors.toSet()));
    }
    return transaction;
  }
}
