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
package org.gradoop.flink.model.impl.operators.propertytransformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Creates a graph with the same structure but a specified property of an element is transformed
 * by the declared function. The transformed value can be stored under a new key. If the
 * original key shall be reused the old value can be stored under the key 'key__x' where 'x' is a
 * version number. This number increases on every continuous transformation.
 * <p>
 * Consider the following example with history enabled:
 * <p>
 * Input vertices:<br>
 * (0, "Person", {yob: 1973})<br>
 * (1, "Person", {yob: 1977})<br>
 * (2, "Person", {yob: 1984})<br>
 * (3, "Person", {yob: 1989})<br>
 * <p>
 * Output vertices (transformed to determine century of their year of birth):<br>
 * (0, "Person", {yob: 1970, yob__1: 1973})<br>
 * (1, "Person", {yob: 1970, yob__1: 1977})<br>
 * (2, "Person", {yob: 1980, yob__1: 1984})<br>
 * (3, "Person", {yob: 1980, yob__1: 1989})<br>
 * <p>
 * This example shows that this operation may be used as pre processing for the
 * {@link org.gradoop.flink.model.impl.operators.grouping.Grouping} operator.
 */
public class PropertyTransformation implements UnaryGraphToGraphOperator {

  /**
   * Label of the element whose property shall be transformed.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Transformation function which shall be applied to a property of a graph head.
   */
  private PropertyTransformationFunction graphHeadTransformationFunction;
  /**
   * Transformation function which shall be applied to a property of a vertex.
   */
  private PropertyTransformationFunction vertexTransformationFunction;
  /**
   * Transformation function which shall be applied to a property of an edge.
   */
  private PropertyTransformationFunction edgeTransformationFunction;
  /**
   * New property key.
   */
  private String newPropertyKey;
  /**
   * True, if the history of a property key shall be kept.
   */
  private boolean keepHistory;

  /**
   * Valued constructor.
   *
   * @param label                            label of the element whose property shall be
   *                                         transformed
   * @param propertyKey                      property key
   * @param graphHeadTransformationFunction  transformation function which shall be applied to a
   *                                         property of a graph head
   * @param vertexTransformationFunction     transformation function which shall be applied to a
   *                                         property of a vertex
   * @param edgeTransformationFunction       transformation function which shall be applied to a
   *                                         property of an edge
   * @param newPropertyKey                   new property key
   * @param keepHistory                flag to enable versioning
   */
  public PropertyTransformation(String label, String propertyKey,
      PropertyTransformationFunction graphHeadTransformationFunction,
      PropertyTransformationFunction vertexTransformationFunction,
      PropertyTransformationFunction edgeTransformationFunction, String newPropertyKey,
      boolean keepHistory) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.graphHeadTransformationFunction = graphHeadTransformationFunction;
    this.vertexTransformationFunction = vertexTransformationFunction;
    this.edgeTransformationFunction = edgeTransformationFunction;
    this.newPropertyKey = newPropertyKey;
    this.keepHistory = keepHistory;
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
   * Applies the property transformation functions on the given datasets.
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

    DataSet<GraphHead> transformedGraphHeads = graphHeadTransformationFunction != null ?
      graphHeads.map(new PropertyTransformationBase<>(label, propertyKey,
          graphHeadTransformationFunction, newPropertyKey, keepHistory))
        .returns(TypeExtractor.createTypeInfo(
          config.getGraphHeadFactory().getType())) : graphHeads;

    DataSet<Vertex> transformedVertices = vertexTransformationFunction != null ?
      vertices.map(new PropertyTransformationBase<>(label, propertyKey,
          vertexTransformationFunction, newPropertyKey, keepHistory))
        .returns(TypeExtractor.createTypeInfo(
          config.getVertexFactory().getType())) : vertices;

    DataSet<Edge> transformedEdges = edgeTransformationFunction != null ?
      edges.map(new PropertyTransformationBase<>(label, propertyKey,
          edgeTransformationFunction, newPropertyKey, keepHistory))
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
    return PropertyTransformation.class.getName();
  }

}
