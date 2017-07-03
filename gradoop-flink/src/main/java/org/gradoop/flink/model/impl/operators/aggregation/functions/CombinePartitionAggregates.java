
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Iterator;

/**
 * partitionAggregateValue,.. => globalAggregateValue
 */
public class CombinePartitionAggregates
  implements GroupReduceFunction<PropertyValue, PropertyValue> {

  /**
   * Aggregate Function
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Constructor.
   *
   * @param aggregateFunction aggregate function
   */
  public CombinePartitionAggregates(AggregateFunction aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
  }

  @Override
  public void reduce(Iterable<PropertyValue> partitionAggregates,
    Collector<PropertyValue> out) throws Exception {

    Iterator<PropertyValue> iterator = partitionAggregates.iterator();

    PropertyValue aggregate = iterator.next();

    while (iterator.hasNext()) {
      aggregateFunction.aggregate(aggregate, iterator.next());
    }

    out.collect(aggregate);
  }
}
