package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMElement;

public class PropertySetter<EL extends EPGMElement>
  implements MapFunction<EL, EL> {

  private final String key;
  private final Object value;

  public PropertySetter(String key, Object value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public EL map(EL element) throws Exception {
    element.setProperty(key, value);
    return element;
  }
}
