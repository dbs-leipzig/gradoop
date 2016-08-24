package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import java.util.Iterator;

public class CombinePartitionApplyAggregates implements
  GroupReduceFunction<Tuple2<GradoopId, PropertyValue>, Tuple2<GradoopId,
    PropertyValue>> {
  private final AggregateFunction aggFunc;

  public CombinePartitionApplyAggregates(
    AggregateFunction aggregateFunction) {

    this.aggFunc = aggregateFunction;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, PropertyValue>> values,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {

    Iterator<Tuple2<GradoopId, PropertyValue>> iterator = values.iterator();

    Tuple2<GradoopId, PropertyValue> aggregate = iterator.next();

    while (iterator.hasNext()) {
      aggregate.f1 = aggFunc.aggregate(aggregate.f1, iterator.next().f1);
    }

    out.collect(aggregate);
  }
}
