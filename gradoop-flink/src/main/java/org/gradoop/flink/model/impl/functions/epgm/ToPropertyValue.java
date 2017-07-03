package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Wraps a value in a property value.
 * @param <T> value type
 */
public class ToPropertyValue<T> implements MapFunction<T, PropertyValue> {

  @Override
  public PropertyValue map(T t) throws Exception {
    return PropertyValue.create(t);
  }
}
