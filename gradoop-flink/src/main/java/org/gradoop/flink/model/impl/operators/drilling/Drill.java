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

    public DrillBuilder setLabel(String label) {
      this.label = label;
      return this;
    }

    public DrillBuilder setPropertyKey(String propertyKey) {
      this.propertyKey = propertyKey;
      return this;
    }

    public DrillBuilder setFunction(DrillFunction function) {
      this.function = function;
      return this;
    }

    public DrillBuilder setNewPropertyKey(String newPropertyKey) {
      this.newPropertyKey = newPropertyKey;
      return this;
    }

    public DrillBuilder drillVertex(boolean drillVertex) {
      this.drillVertex = drillVertex;
      return this;
    }

    public DrillBuilder drillEdge(boolean drillEdge) {
      this.drillVertex = !drillEdge;
      return this;
    }

    public DrillDown buildDrillDown() {
      Objects.requireNonNull(propertyKey);
      return new DrillDown(label, propertyKey, function, newPropertyKey, drillVertex);
    }

    public RollUp buildRollUp() {
      Objects.requireNonNull(propertyKey);
      Objects.requireNonNull(function);
      return new RollUp(label, propertyKey, function, newPropertyKey, drillVertex);
    }
  }
}
