package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class BaseAggregateFunction<T extends Element> implements AggregateFunction<T> {
  /**
   * Key of the aggregate property.
   */
  private String aggregatePropertyKey;

  /**
   * Constructor.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public BaseAggregateFunction(String aggregatePropertyKey) {
    setAggregatePropertyKey(aggregatePropertyKey);
  }

  /**
   * Sets the property key used to store the aggregate value.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public void setAggregatePropertyKey(String aggregatePropertyKey) {
    this.aggregatePropertyKey = aggregatePropertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return aggregatePropertyKey;
  }
}
