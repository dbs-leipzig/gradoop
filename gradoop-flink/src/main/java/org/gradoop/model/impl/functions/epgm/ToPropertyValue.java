package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.properties.PropertyValue;

public class ToPropertyValue<T> implements MapFunction<T, PropertyValue> {

  @Override
  public PropertyValue map(T t) throws Exception {
    return PropertyValue.create(t);
  }
}
