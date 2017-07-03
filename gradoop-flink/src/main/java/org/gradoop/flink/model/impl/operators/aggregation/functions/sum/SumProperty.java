
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass if aggregate functions that sum property values of vertices OR
 * edges.
 */
public abstract class SumProperty extends Sum implements AggregateFunction {

  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public SumProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return "sum_" + propertyKey;
  }
}
