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

import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Creates a graph with the same structure but a specified property of an element is transformed
 * by the declared function. The transformed value can be stored under a new key. If the
 * original key shall be reused the old value can be stored under the key 'key__x' where 'x' is a
 * version number. This number increases on every continuous transformation.
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
   * @param propertyKey                      property key
   * @param graphHeadTransformationFunction  transformation function which shall be applied to a
   *                                         property of a graph head
   * @param vertexTransformationFunction     transformation function which shall be applied to a
   *                                         property of a vertex
   * @param edgeTransformationFunction       transformation function which shall be applied to a
   *                                         property of an edge
   * @param label                            label of the element whose property shall be
   *                                         transformed (optional)
   * @param newPropertyKey                   new property key (optional)
   * @param keepHistory                      flag to enable versioning (false by default)
   */
  public PropertyTransformation(String propertyKey,
      PropertyTransformationFunction graphHeadTransformationFunction,
      PropertyTransformationFunction vertexTransformationFunction,
      PropertyTransformationFunction edgeTransformationFunction, String label,
      String newPropertyKey, boolean keepHistory) {
    if (graphHeadTransformationFunction == null && vertexTransformationFunction == null &&
        edgeTransformationFunction == null) {
      throw new IllegalArgumentException("Provide at least one transformation function.");
    }

    this.label = label;
    this.propertyKey = propertyKey;
    this.graphHeadTransformationFunction = graphHeadTransformationFunction;
    this.vertexTransformationFunction = vertexTransformationFunction;
    this.edgeTransformationFunction = edgeTransformationFunction;
    this.newPropertyKey = newPropertyKey;
    this.keepHistory = keepHistory;
  }

  /**
   * Creates a new property transformation operator instance.
   *
   * @param propertyKey                      property key
   * @param graphHeadTransformationFunction  transformation function which shall be applied to a
   *                                         property of a graph head
   * @param vertexTransformationFunction     transformation function which shall be applied to a
   *                                         property of a vertex
   * @param edgeTransformationFunction       transformation function which shall be applied to a
   *                                         property of an edge
   */
  public PropertyTransformation(String propertyKey,
      PropertyTransformationFunction graphHeadTransformationFunction,
      PropertyTransformationFunction vertexTransformationFunction,
      PropertyTransformationFunction edgeTransformationFunction) {
    this(propertyKey, graphHeadTransformationFunction, vertexTransformationFunction,
         edgeTransformationFunction, null, null, false);
  }

  /**
   * Applies the property transformation functions on the given input graph.
   *
   * @param graph input graph
   * @return transformed logical graph
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    return graph.transform(
        graphHeadTransformationFunction == null ? null : new BasePropertyTransformationFunction<>(
            propertyKey, graphHeadTransformationFunction, label, newPropertyKey, keepHistory),
        vertexTransformationFunction == null ? null : new BasePropertyTransformationFunction<>(
            propertyKey, vertexTransformationFunction, label, newPropertyKey, keepHistory),
        edgeTransformationFunction == null ? null : new BasePropertyTransformationFunction<>(
            propertyKey, edgeTransformationFunction, label, newPropertyKey, keepHistory));
  }

  @Override
  public String getName() {
    return PropertyTransformation.class.getName();
  }

}
