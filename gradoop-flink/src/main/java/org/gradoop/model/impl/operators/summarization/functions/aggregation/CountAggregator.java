package org.gradoop.model.impl.operators.summarization.functions.aggregation;

import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Special sum aggregator to count elements.
 */
public class CountAggregator extends PropertyValueAggregator {

  /**
   * Default property key to fetch values for aggregation.
   */
  public static final String DEFAULT_PROPERTY_KEY = "*";

  /**
   * Default property key to store the result of the aggregate function.
   */
  public static final String DEFAULT_AGGREGATE_PROPERTY_KEY = "count";

  /**
   * Aggregate value to count the number of calls of {@link #aggregate(Object)}.
   */
  private Long aggregate;

  /**
   * Creates a new count aggregator
   */
  public CountAggregator() {
    this(DEFAULT_PROPERTY_KEY, DEFAULT_AGGREGATE_PROPERTY_KEY);
  }

  /**
   * Creates a new count aggregator
   *
   * @param aggregatePropertyKey used to store the final aggregate value
   */
  public CountAggregator(String aggregatePropertyKey) {
    this(DEFAULT_PROPERTY_KEY, aggregatePropertyKey);
  }

  /**
   * Creates a new count aggregator
   *
   * @param propertyKey           used to define the property to aggregate
   * @param aggregatePropertyKey  used to store the final aggregate value
   */
  public CountAggregator(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey, aggregatePropertyKey);
    aggregate = 0L;
  }

  @Override
  public void aggregate(PropertyValue value) {
    aggregateInternal(value);
  }

  @Override
  protected boolean isInitialized() {
    return true;
  }

  @Override
  protected void initializeAggregate(PropertyValue value) {
    aggregate = 0L;
  }

  @Override
  protected void aggregateInternal(PropertyValue value) {
    aggregate++;
  }

  @Override
  protected PropertyValue getAggregateInternal() {
    return PropertyValue.create(aggregate);
  }

  @Override
  public void resetAggregate() {
    aggregate = 0L;
  }
}
