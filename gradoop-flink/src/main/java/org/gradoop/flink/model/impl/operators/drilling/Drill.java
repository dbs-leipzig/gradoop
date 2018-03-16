/**
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
   * Label of the element whose property shall be drilled.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Drill function which shall be applied to a property.
   */
  private DrillFunction function;
  /**
   * New property key.
   */
  private String newPropertyKey;
  /**
   * True, if vertices shall be drilled, false for edges
   */
  private boolean drillVertex;

  /**
   * Valued constructor.
   * @param label                  label of the element whose property shall be drilled
   * @param propertyKey             property key
   * @param function                drill function which shall be applied to a property
   * @param newPropertyKey          new property key
   * @param drillVertex             true, if vertices shall be drilled, false for edges
   */
  Drill(String label, String propertyKey, DrillFunction function, String newPropertyKey,
    boolean drillVertex) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.function = function;
    this.newPropertyKey = newPropertyKey;
    this.drillVertex = drillVertex;
  }

  protected String getLabel() {
    return label;
  }

  protected String getPropertyKey() {
    return propertyKey;
  }

  protected DrillFunction getFunction() {
    return function;
  }

  protected String getNewPropertyKey() {
    return newPropertyKey;
  }

  /**
   * True, if vertices shall be drilled, false for edges
   *
   * @return true for vertex drilling
   */
  protected boolean drillVertex() {
    return drillVertex;
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
     * Label of the element whose property shall be drilled.
     */
    private String label;
    /**
     * Property key.
     */
    private String propertyKey;
    /**
     * Drill function which shall be applied to a property.
     */
    private DrillFunction function;
    /**
     * New property key.
     */
    private String newPropertyKey;
    /**
     * True, if vertices shall be drilled, false for edges
     */
    private boolean drillVertex;

    /**
     * Creates the drill up / drill down class. By default the vertices will be transformed, note
     * that {@link DrillBuilder#drillVertex{boolean} and
     * {@link DrillBuilder#drillEdge(boolean)} negate each other so only the last used will be
     * considered. In case of drill down where the function is not set it is necessary that a
     * drill up function was used before.
     */
    public DrillBuilder() {
      function = null;
      drillVertex = true;
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
     * @param function drill function
     * @return the modified drill builder
     */
    public DrillBuilder setFunction(DrillFunction function) {
      this.function = function;
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
     * Specifies if a vertex shall be drilled. Negates edge drilling.
     *
     * @param drillVertex true to enable vertex drilling
     * @return the modified drill builder
     */
    public DrillBuilder drillVertex(boolean drillVertex) {
      this.drillVertex = drillVertex;
      return this;
    }

    /**
     * Specifies if an edge shall be drilled. Negates vertex drilling.
     *
     * @param drillEdge true to enable edge drilling
     * @return the modified drill builder
     */
    public DrillBuilder drillEdge(boolean drillEdge) {
      this.drillVertex = !drillEdge;
      return this;
    }

    /**
     * Creates a drill up operation.
     *
     * @return drill up operation
     */
    public DrillUp buildDrillUp() {
      Objects.requireNonNull(propertyKey);
      Objects.requireNonNull(function);
      return new DrillUp(
        label, propertyKey, function, newPropertyKey, drillVertex);
    }

    /**
     * Creates a drill down operation.
     *
     * @return drill down operation
     */
    public DrillDown buildDrillDown() {
      Objects.requireNonNull(propertyKey);
      return new DrillDown(
        label, propertyKey, function, newPropertyKey, drillVertex);
    }
  }
}
