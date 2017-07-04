
package org.gradoop.flink.model.impl.operators.grouping.functions.aggregation;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Used to find the maximum value in a set of values.
 */
public class MaxAggregator extends PropertyValueAggregator {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Aggregate value. No need to deserialize as it is just used for comparison.
   */
  private PropertyValue aggregate;

  /**
   * Creates a new aggregator
   *
   * @param propertyKey          property key to access values
   * @param aggregatePropertyKey property key for final aggregate value
   */
  public MaxAggregator(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  @Override
  protected boolean isInitialized() {
    return aggregate != null;
  }

  @Override
  protected void aggregateInternal(PropertyValue value) {
    if (value.compareTo(aggregate) > 0) {
      aggregate = value;
    }
  }

  @Override
  protected PropertyValue getAggregateInternal() {
    return aggregate;
  }

  @Override
  protected void initializeAggregate(PropertyValue value) {
    aggregate = value;
  }

  @Override
  public void resetAggregate() {
    aggregate = null;
  }
}
