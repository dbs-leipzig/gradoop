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
public class PropertyValueFloatBenchmark {

  private PropertyValue VALUE;
  private PropertyValue FLOAT_VALUE;
  private Float FLOAT;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    FLOAT_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_FLOAT, 0x3f});
    FLOAT = (float) 1.5;
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(FLOAT);
  }

  @Benchmark
  public void set() {
    VALUE.setFloat(FLOAT);
  }

  @Benchmark
  public Boolean is() {
    return FLOAT_VALUE.isFloat();
  }

  @Benchmark
  public Float get() {
    return FLOAT_VALUE.getFloat();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(FLOAT);
  }

  @Benchmark
  public Class<?> getType() {
    return FLOAT_VALUE.getType();
  }
}