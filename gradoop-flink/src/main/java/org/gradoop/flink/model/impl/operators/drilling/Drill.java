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
package org.gradoop.flink.model.impl.operators.drilling;

import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

import java.util.Objects;

/**
 * Base class for the drill up / drill down operation. The drill operator gives the opportunity to
 * modify a property value of graph elements. This modification may be applied on all elements of
 * a kind (vertex / edge) or only on elements with a specific label. The old value will be stored
 * on the element to be able to restore the values later.
 */
public abstract class Drill implements UnaryGraphToGraphOperator {

  /**
   * Supported elements.
   */
  public enum Element {
    /**
     * Vertices
     */
    VERTICES,
    /**
     * Edges
     */
    EDGES,
    /**
     * Graph head
     */
    GRAPHHEAD
  }
  /**
   * Element to be covered by the operation.
   */
  private Element element;
  /**
   * Label of the element whose property shall be drilled.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Drill function which shall be applied to a property of a vertex.
   */
  private DrillFunction vertexDrillFunction;
  /**
   * Drill function which shall be applied to a property of an edge.
   */
  private DrillFunction edgeDrillFunction;
  /**
   * Drill function which shall be applied to a property of a graph head.
   */
  private DrillFunction graphheadDrillFunction;
  /**
   * New property key.
   */
  private String newPropertyKey;

  /**
   * Valued constructor.
   *
   * @param label                   label of the element whose property shall be drilled
   * @param propertyKey             property key
   * @param vertexDrillFunction     drill function which shall be applied to a property of a vertex
   * @param edgeDrillFunction       drill function which shall be applied to a property of an edge
   * @param graphheadDrillFunction  drill function which shall be applied to a property of a
   *                                graph head
   * @param newPropertyKey          new property key
   * @param element                 Element to be covered by the operation
   */
  Drill(String label, String propertyKey, DrillFunction vertexDrillFunction,
    DrillFunction edgeDrillFunction, DrillFunction graphheadDrillFunction,
    String newPropertyKey, Element element) {
    this.element = element != null ? element : Element.VERTICES;
    this.label = label;
    this.propertyKey = propertyKey;
    this.vertexDrillFunction = vertexDrillFunction;
    this.edgeDrillFunction = edgeDrillFunction;
    this.graphheadDrillFunction = graphheadDrillFunction;
    this.newPropertyKey = newPropertyKey;
  }

  protected Element getElement() {
    return element;
  }

  protected String getLabel() {
    return label;
  }

  protected String getPropertyKey() {
    return propertyKey;
  }

  protected DrillFunction getVertexDrillFunction() {
    return vertexDrillFunction;
  }

  protected DrillFunction getEdgeDrillFunction() {
    return edgeDrillFunction;
  }

  protected DrillFunction getGraphheadDrillFunction() {
    return graphheadDrillFunction;
  }

  protected String getNewPropertyKey() {
    return newPropertyKey;
  }

  /**
   * True, if all elements of a kind (vertex / edge) shall be drilled.
   *
   * @return true for drilling all elements
   */
  protected boolean drillAllLabels() {
    return label == null;
  }

  /**
   * True, if the current property key shall be reused.
   *
   * @return true for keeping the property key
   */
  protected boolean keepCurrentPropertyKey() {
    return newPropertyKey == null;
  }

  /**
   * Used for building a drill operator instance.
   */
  public static class DrillBuilder {
    /**
     * Element to be covered by the operation.
     */
    private Element element;
    /**
     * Label of the element whose property shall be drilled.
     */
    private String label;
    /**
     * Property key.
     */
    private String propertyKey;
    /**
     * Drill function which shall be applied to a property of a vertex.
     */
    private DrillFunction vertexDrillFunction;
    /**
     * Drill function which shall be applied to a property of an edge.
     */
    private DrillFunction edgeDrillFunction;
    /**
     * Drill function which shall be applied to a property of a graph head.
     */
    private DrillFunction graphheadDrillFunction;
    /**
     * New property key.
     */
    private String newPropertyKey;

    /**
     * Creates the drill up / drill down class. Note that only one DrillFunction must be set.
     * In case of drill down where the function is not set it is necessary that a
     * drill up function was used before.
     */
    public DrillBuilder() {
      vertexDrillFunction = null;
      edgeDrillFunction = null;
      graphheadDrillFunction = null;
      element = null;
    }

    /**
     * Specifies the element to be covered by the operation.
     *
     * @param element element to be covered by the operation
     * @return the modified drill builder
     */
    public DrillBuilder setElement(Element element) {
      this.element = element;
      return this;
    }

    /**
     * Specifies the label of the element whose property shall be drilled.
     *
     * @param label element label
     * @return the modified drill builder
     */
    public DrillBuilder setLabel(String label) {
      this.label = label;
      return this;
    }

    /**
     * Specifies the property key whose value shall be drilled.
     *
     * @param propertyKey property key
     * @return the modified drill builder
     */
    public DrillBuilder setPropertyKey(String propertyKey) {
      this.propertyKey = propertyKey;
      return this;
    }

    /**
     * Specifies the function to calculate the new value.
     *
     * <p>
     * Note: Only one DrillFunction must be set and thus this function unset's all other
     * possibly set DrillFunction's.
     * </p>
     *
     * @param vertexDrillFunction the drill function for vertices
     * @return the modified drill builder
     */
    public DrillBuilder setVertexDrillFunction(DrillFunction vertexDrillFunction) {
      this.vertexDrillFunction = vertexDrillFunction;
      this.edgeDrillFunction = null;
      this.graphheadDrillFunction = null;
      this.element = Element.VERTICES;
      return this;
    }

    /**
     * Specifies the function to calculate the new value.
     *
     * <p>
     * Note: Only one DrillFunction must be set and thus this function unset's all other
     * possibly set DrillFunction's.
     * </p>
     *
     * @param edgeDrillFunction the drill function for edges
     * @return the modified drill builder
     */
    public DrillBuilder setEdgeDrillFunction(DrillFunction edgeDrillFunction) {
      this.edgeDrillFunction = edgeDrillFunction;
      this.vertexDrillFunction = null;
      this.graphheadDrillFunction = null;
      this.element = Element.EDGES;
      return this;
    }

    /**
     * Specifies the function to calculate the new value.
     *
     * <p>
     * Note: Only one DrillFunction must be set and thus this function unset's all other
     * possibly set DrillFunction's.
     * </p>
     *
     * @param graphheadDrillFunction the drill function for graph heads
     * @return the modified drill builder
     */
    public DrillBuilder setGraphheadDrillFunction(DrillFunction graphheadDrillFunction) {
      this.graphheadDrillFunction = graphheadDrillFunction;
      this.vertexDrillFunction = null;
      this.edgeDrillFunction = null;
      this.element = Element.GRAPHHEAD;
      return this;
    }

    /**
     * Specifies the new key of the drilled value.
     *
     * @param newPropertyKey new property key
     * @return the modified drill builder
     */
    public DrillBuilder setNewPropertyKey(String newPropertyKey) {
      this.newPropertyKey = newPropertyKey;
      return this;
    }

    /**
     * Creates a roll up operation.
     *
     * @return roll up operation
     */
    public RollUp buildRollUp() {
      Objects.requireNonNull(propertyKey);

      if (vertexDrillFunction == null &&
          edgeDrillFunction == null &&
          graphheadDrillFunction == null) {
        throw new IllegalArgumentException();
      }

      return new RollUp(
        label, propertyKey, vertexDrillFunction, edgeDrillFunction, graphheadDrillFunction,
        newPropertyKey, element);
    }

    /**
     * Creates a drill down operation.
     *
     * @return drill down operation
     */
    public DrillDown buildDrillDown() {
      Objects.requireNonNull(propertyKey);
      return new DrillDown(
        label, propertyKey, vertexDrillFunction, edgeDrillFunction, graphheadDrillFunction,
        newPropertyKey, element);
    }
  }
}
