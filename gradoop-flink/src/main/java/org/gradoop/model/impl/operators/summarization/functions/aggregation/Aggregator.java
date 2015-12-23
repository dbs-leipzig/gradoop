package org.gradoop.model.impl.operators.summarization.functions.aggregation;

import org.gradoop.model.impl.properties.PropertyValue;

import java.io.Serializable;

/**
 * Defines an aggregate function that can be applied on vertex and edge groups
 * during {@link org.gradoop.model.impl.operators.summarization.Summarization}.
 *
 * @param <IN> input type for the specific aggregation function
 */
public interface Aggregator<IN> extends Serializable {
  /**
   * Adds the given value to the aggregate.
   *
   * @param value value to aggregate
   */
  void aggregate(IN value);
  /**
   * Returns the final aggregate.
   *
   * @return aggregate
   */
  PropertyValue getAggregate();
  /**
   * Returns the key of the property which is being aggregated (e.g. age, price)
   *
   * @return property key of the value to be aggregated
   */
  String getPropertyKey();
  /**
   * Returns the property key, which is used to store the final aggregate value
   * (e.g. COUNT, AVG(age), ...)
   *
   * @return property key to store the resulting aggregate value
   */
  String getAggregatePropertyKey();
  /**
   * Resets the internal aggregate value.
   */
  void resetAggregate();
}
