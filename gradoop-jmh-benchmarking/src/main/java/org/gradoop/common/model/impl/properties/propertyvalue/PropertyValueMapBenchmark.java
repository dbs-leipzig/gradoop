package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueMapBenchmark {

  private PropertyValue VALUE;
  private PropertyValue MAP_VALUE;
  private Map<PropertyValue, PropertyValue> MAP;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    MAP_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_MAP, 0xf});
    MAP = new HashMap<>();
    MAP.put(PropertyValue.create("A"), PropertyValue.create("B"));
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(MAP);
  }

  @Benchmark
  public void set() {
    VALUE.setMap(MAP);
  }

  @Benchmark
  public Boolean is() {
    return MAP_VALUE.isMap();
  }

  @Benchmark
  public Map<PropertyValue, PropertyValue> get() {
    return MAP_VALUE.getMap();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(MAP);
  }

  @Benchmark
  public Class<?> getType() {
    return MAP_VALUE.getType();
  }
}
