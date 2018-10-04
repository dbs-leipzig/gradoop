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
public class PropertyValueDoubleBenchmark {

  private PropertyValue VALUE;
  private PropertyValue DOUBLE_VALUE;
  private Double DOUBLE;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    DOUBLE_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_DOUBLE, 0x3f});
    DOUBLE = 1.5;
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(1.5);
  }

  @Benchmark
  public void set() {
    VALUE.setDouble(DOUBLE);
  }

  @Benchmark
  public Boolean is() {
    return DOUBLE_VALUE.isDouble();
  }

  @Benchmark
  public Double get() {
    return DOUBLE_VALUE.getDouble();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(DOUBLE);
  }

  @Benchmark
  public Class<?> getType() {
    return DOUBLE_VALUE.getType();
  }
}