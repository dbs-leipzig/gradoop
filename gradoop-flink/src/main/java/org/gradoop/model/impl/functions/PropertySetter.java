package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMElement;

public class PropertySetter<EL extends EPGMElement>
  extends RichMapFunction<EL, EL> {

  public static final String KEY = "key";
  public static final String VALUE = "value";

  private String key;
  private Object value;

  public PropertySetter(String key) {
    this.key = key;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.value = getRuntimeContext()
      .getBroadcastVariable(VALUE).get(0);
  }

  @Override
  public EL map(EL element) throws Exception {
    element.setProperty(key, value);
    return element;
  }
}
