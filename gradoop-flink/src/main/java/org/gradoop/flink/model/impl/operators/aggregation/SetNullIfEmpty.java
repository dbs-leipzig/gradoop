package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Iterator;

public class SetNullIfEmpty
  implements GroupReduceFunction<PropertyValue, PropertyValue> {
  @Override

  public void reduce(Iterable<PropertyValue> values,
    Collector<PropertyValue> out) throws Exception {

    Iterator<PropertyValue> iterator = values.iterator();

    PropertyValue value = iterator.next();

    if (iterator.hasNext() && value.equals(PropertyValue.NULL_VALUE)) {
      value = iterator.next();
    }

    out.collect(value);
  }
}
