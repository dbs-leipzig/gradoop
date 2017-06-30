/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
     * Creates the drill down or roll up class. By default will the vertices be transformed, not
     * that {@link DrillBuilder#drillVertex{boolean} and
     * {@link DrillBuilder#drillEdge(boolean)} negate each other so only the last used will be
     * considered. In case of drill down where the function is not set it is necessary that a
     * roll up function was used before.
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
     * Creates a drill down operation.
     *
     * @return drill down operation
     */
    public DrillDown buildDrillDown() {
      Objects.requireNonNull(propertyKey);
      return new DrillDown(label, propertyKey, function, newPropertyKey, drillVertex);
    }

    /**
     * Creates a roll up operation.
     *
     * @return rol up operation
     */
    public RollUp buildRollUp() {
      Objects.requireNonNull(propertyKey);
      Objects.requireNonNull(function);
      return new RollUp(label, propertyKey, function, newPropertyKey, drillVertex);
    }
  }
}
