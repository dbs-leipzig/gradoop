
package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

/**
 * Superclass of aggregate functions that determine a minimal property value.
 */
public abstract class MinProperty extends Min {
  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public MinProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return "min_" + propertyKey;
  }
}
