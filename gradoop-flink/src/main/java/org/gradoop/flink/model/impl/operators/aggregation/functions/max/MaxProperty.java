
package org.gradoop.flink.model.impl.operators.aggregation.functions.max;

/**
 * Superclass of aggregate functions that determine a maximal property value.
 */
public abstract class MaxProperty extends Max {
  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public MaxProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return "max_" + propertyKey;
  }
}
