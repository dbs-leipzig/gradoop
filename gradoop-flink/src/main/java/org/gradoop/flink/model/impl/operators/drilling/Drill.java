/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
 * Base class for the roll up / drill down operation. Also contains a builder for these.
 */
public abstract class Drill implements UnaryGraphToGraphOperator {

  /**
   * Used as new property key to declare that the current property key shall be kept.
   */
  public static final String KEEP_CURRENT_PROPERTY_KEY = ":keepKey";
  /**
   * Used as label to declare that all vertices or all edges shall be drilled.
   */
  public static final String DRILL_ALL_ELEMENTS = ":allElements";
  /**
   * Separator between the iteration number and the original property key when the property key
   * shall be kept.
   */
  public static final String PROPERTY_VERSION_SEPARATOR = "__";

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
   *  @param label label of the element whose property shall be drilled, or
   *              see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey property key
   * @param function drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   * @param drillVertex true, if vertices shall be drilled, false for edges
   */
  public Drill(String label, String propertyKey, DrillFunction function, String newPropertyKey,
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

  protected boolean isDrillVertex() {
    return drillVertex;
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
     * Creates the roll up class. By default the vertices will be transformed, note
     * that {@link DrillBuilder#drillVertex{boolean} and
     * {@link DrillBuilder#drillEdge(boolean)} negate each other so only the last used will be
     * considered.
     */
    public DrillBuilder() {
      label = DRILL_ALL_ELEMENTS;
      function = null;
      newPropertyKey = Drill.KEEP_CURRENT_PROPERTY_KEY;
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
     * Creates a roll up operation.
     *
     * @return roll up operation
     */
    public RollUp buildRollUp() {
      Objects.requireNonNull(propertyKey);
      Objects.requireNonNull(function);
      return new RollUp(label, propertyKey, function, newPropertyKey, drillVertex);
    }
  }
}
