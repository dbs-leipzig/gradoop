package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;

public interface AggregateDefaultValue {
  public PropertyValue getDefaultValue();
}
