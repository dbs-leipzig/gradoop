package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.AggregateFunction;

public abstract class BaseAggregateFunction<T extends Element> implements AggregateFunction<T> {

  private String aggregatePropertyKey;

  public BaseAggregateFunction(String aggregatePropertyKey) {
    setAggregatePropertyKey(aggregatePropertyKey);
  }

  public void setAggregatePropertyKey(String aggregatePropertyKey) {
    this.aggregatePropertyKey = aggregatePropertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return aggregatePropertyKey;
  }
}
