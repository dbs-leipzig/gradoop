package org.gradoop.flink.model.impl.operators.aggregation.functions.count;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.ElementAggregateFunction;

/**
 * Aggregate function returning the element count of a graph / graph collection.
 */
public class Count extends BaseCount<Element> implements ElementAggregateFunction {

  public Count() {
    super("count");
  }

  public Count(String aggregatePropertyKey) {
    super(aggregatePropertyKey);
  }
}
