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

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Creates a graph with the same structure but a specified property of an element is transformed
 * by the declared function. The transformed value can be stored under a new key. If the
 * original key shall be reused the old value is stored under the key 'key__x' where 'x' is a
 * version number. This number increases on every continuous transformation.
 * <p>
 * Consider the following example:
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
 *
 * @param <EL> graph element to be considered by the transformation
 */
public class PropertyTransformation<EL extends Element> implements TransformationFunction<EL>, UnaryGraphToGraphOperator {

  /**
   * Separator between the iteration number and the original property key when the property key
   * shall be kept.
   */
  static final String PROPERTY_VERSION_SEPARATOR = "__";

  /**
   * Label of the element whose property shall be transformed.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Transformation function which shall be applied to a property of a vertex.
   */
  private PropertyTransformationFunction vertexTransformationFunction;
  /**
   * Transformation function which shall be applied to a property of an edge.
   */
  private PropertyTransformationFunction edgeTransformationFunction;
  /**
   * Transformation function which shall be applied to a property of a graph head.
   */
  private PropertyTransformationFunction graphHeadTransformationFunction;
  /**
   * New property key.
   */
  private String newPropertyKey;
  /**
   * True, if all elements if a kind (vertex / edge / graphHead) shall be transformed.
   */
  private boolean transformAllLabels;
  /**
   * True, if the current property key shall be reused.
   */
  private boolean keepCurrentPropertyKey;

  /**
   * Valued constructor.
   *
   * @param label                            label of the element whose property shall be
   *                                         transformed
   * @param propertyKey                      property key
   * @param vertexTransformationFunction     transformation function which shall be applied to a
   *                                         property of a vertex
   * @param edgeTransformationFunction       transformation function which shall be applied to a
   *                                         property of an edge
   * @param graphHeadTransformationFunction  transformation function which shall be applied to a
   *                                         property of a graph head
   * @param newPropertyKey                   new property key
   */
  public PropertyTransformation(String label, String propertyKey,
      PropertyTransformationFunction vertexTransformationFunction,
      PropertyTransformationFunction edgeTransformationFunction,
      PropertyTransformationFunction graphHeadTransformationFunction, String newPropertyKey) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.vertexTransformationFunction = vertexTransformationFunction;
    this.edgeTransformationFunction = edgeTransformationFunction;
    this.graphHeadTransformationFunction = graphHeadTransformationFunction;
    this.newPropertyKey = newPropertyKey;
    this.transformAllLabels = label == null;
    this.keepCurrentPropertyKey = newPropertyKey == null;
  }

  @Override
  public EL apply(EL current, EL transformed) {
    PropertyTransformationFunction function = null;
    if (vertexTransformationFunction != null) {
      function = vertexTransformationFunction;
    } else if (edgeTransformationFunction != null) {
      function = edgeTransformationFunction;
    } else if (graphHeadTransformationFunction != null) {
      function = graphHeadTransformationFunction;
    } else {
      throw new IllegalArgumentException(
        "You must supply exactly one ProppertyTransformationFunction!");
    }

    // transformed will have the current id and graph ids, but not the label or the properties
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    // filters relevant elements
    if (transformAllLabels || label.equals(current.getLabel())) {
      if (current.hasProperty(propertyKey)) {
        // save transformed value with the same key
        if (keepCurrentPropertyKey) {
          // save the original value with the version number in the property key
          transformed.setProperty(
            propertyKey + PROPERTY_VERSION_SEPARATOR + getNextVersionNumber(current),
            current.getPropertyValue(propertyKey));
          // save the new transformed value
          transformed.setProperty(
            propertyKey,
            function.execute(current.getPropertyValue(propertyKey)));
          // new key is used, so the old property is untouched
        } else {
          // store the transformed value with the new key
          transformed.setProperty(
            newPropertyKey,
            function.execute(current.getPropertyValue(propertyKey)));
        }
      }
    }
    return transformed;
  }

  /**
   * Returns the next unused version number.
   *
   * @param element element whose property shall be transformed up
   * @return next unused version number
   */
  protected int getNextVersionNumber(EL element) {
    int i = 1;
    while (element.hasProperty(propertyKey + PROPERTY_VERSION_SEPARATOR + i)) {
      i++;
    }
    return i;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (vertexTransformationFunction != null) {
      graph = graph.transformVertices((TransformationFunction<Vertex>) this);
    } else if (edgeTransformationFunction != null) {
      graph = graph.transformEdges((TransformationFunction<Edge>) this);
    } else if (graphHeadTransformationFunction != null) {
      graph = graph.transformGraphHead((TransformationFunction<GraphHead>) this);
    } else {
      throw new IllegalArgumentException(
        "You must supply exactly one ProppertyTransformationFunction!");
    }
    return graph;
  }

  @Override
  public String getName() {
    return PropertyTransformation.class.getName();
  }

}
