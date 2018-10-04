package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueStringBenchmark {

  private PropertyValue VALUE;
  private PropertyValue STRING_VALUE;
  private String STRING;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    STRING_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_INTEGER, 0xf});
    STRING = "15";
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(STRING);
  }

  @Benchmark
  public void set() {
    VALUE.setString(STRING);
  }

  @Benchmark
  public Boolean is() {
    return STRING_VALUE.isString();
  }

  @Benchmark
  public String get() {
    return STRING_VALUE.getString();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(STRING);
  }

  @Benchmark
  public Class<?> getType() {
    return STRING_VALUE.getType();
  }
}
