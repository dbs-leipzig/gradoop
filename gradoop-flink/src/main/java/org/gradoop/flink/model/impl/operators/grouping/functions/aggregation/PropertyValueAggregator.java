
package org.gradoop.flink.model.impl.operators.grouping.functions.aggregation;

import org.gradoop.common.model.impl.properties.PropertyValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Aggregator that takes {@link PropertyValue} as input for aggregation.
 */
public abstract class PropertyValueAggregator extends BaseAggregator<PropertyValue> {

  /**
   * Creates a new aggregator
   *
   * @param propertyKey           used to fetch property value from elements
   * @param aggregatePropertyKey  used to store the final aggregate value
   */
  protected PropertyValueAggregator(String propertyKey,
    String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
  }

  @Override
  public void aggregate(PropertyValue value) {
    value = checkNotNull(value);
    if (!value.isNull()) {
      if (!isInitialized()) {
        initializeAggregate(value);
      }
      aggregateInternal(value);
    }
  }
}


