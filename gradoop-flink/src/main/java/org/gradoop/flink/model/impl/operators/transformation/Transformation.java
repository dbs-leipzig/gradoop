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
package org.gradoop.flink.model.impl.operators.transformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformEdge;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformGraphHead;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * The modification operators is a unary graph operator that takes a logical
 * graph as input and applies user defined modification functions on the
 * elements of that graph as well as on its graph head.
 *
 * The identity of the elements is preserved.
 */
public class Transformation implements UnaryGraphToGraphOperator {

  /**
   * Modification function for graph heads
   */
  protected final TransformationFunction<GraphHead> graphHeadTransFunc;

  /**
   * Modification function for vertices
   */
  protected final TransformationFunction<Vertex> vertexTransFunc;

  /**
   * Modification function for edges
   */
  protected final TransformationFunction<Edge> edgeTransFunc;

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadTransFunc  graph head transformation function
   * @param vertexTransFunc     vertex transformation function
   * @param edgeTransFunc       edge transformation function
   */
  public Transformation(TransformationFunction<GraphHead> graphHeadTransFunc,
    TransformationFunction<Vertex> vertexTransFunc,
    TransformationFunction<Edge> edgeTransFunc) {

    if (graphHeadTransFunc == null &&
      vertexTransFunc == null &&
      edgeTransFunc == null) {
      throw new IllegalArgumentException(
        "Provide at least one transformation function.");
    }
    this.graphHeadTransFunc = graphHeadTransFunc;
    this.vertexTransFunc    = vertexTransFunc;
    this.edgeTransFunc      = edgeTransFunc;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    return executeInternal(
      graph.getGraphHead(),
      graph.getVertices(),
      graph.getEdges(),
      graph.getConfig());
  }

  /**
   * Applies the transformation functions on the given datasets.
   *
   * @param graphHeads  graph heads
   * @param vertices    vertices
   * @param edges       edges
   * @param config      gradoop flink config
   * @return transformed logical graph
   */
  @SuppressWarnings("unchecked")
  protected LogicalGraph executeInternal(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices, DataSet<Edge> edges, GradoopFlinkConfig config) {

    DataSet<GraphHead> transformedGraphHeads = graphHeadTransFunc != null ?
      graphHeads.map(new TransformGraphHead(
        graphHeadTransFunc, config.getGraphHeadFactory()))
        .returns(TypeExtractor.createTypeInfo(
          config.getGraphHeadFactory().getType())) : graphHeads;

    DataSet<Vertex> transformedVertices = vertexTransFunc != null ?
      vertices.map(new TransformVertex(
        vertexTransFunc, config.getVertexFactory()))
        .returns(TypeExtractor.createTypeInfo(
          config.getVertexFactory().getType())) : vertices;

    DataSet<Edge> transformedEdges = edgeTransFunc != null ?
      edges.map(new TransformEdge(
        edgeTransFunc, config.getEdgeFactory()))
        .returns(TypeExtractor.createTypeInfo(
          config.getEdgeFactory().getType())) : edges;

    return config.getLogicalGraphFactory().fromDataSets(
      transformedGraphHeads,
      transformedVertices,
      transformedEdges
    );
  }

  @Override
  public String getName() {
    return Transformation.class.getName();
  }
}
