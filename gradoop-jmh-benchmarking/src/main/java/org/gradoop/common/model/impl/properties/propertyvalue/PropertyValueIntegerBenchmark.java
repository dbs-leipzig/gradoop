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
public class PropertyValueIntegerBenchmark {

  private PropertyValue VALUE;
  private PropertyValue INTEGER_VALUE;
  private Integer INTEGER;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    INTEGER_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_INTEGER, 0xf});
    INTEGER = 15;
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(1);
  }

  @Benchmark
  public void set() {
    VALUE.setInt(INTEGER);
  }

  @Benchmark
  public Boolean is() {
    return INTEGER_VALUE.isInt();
  }

  @Benchmark
  public Integer get() {
    return INTEGER_VALUE.getInt();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(INTEGER);
  }

  @Benchmark
  public Class<?> getType() {
    return INTEGER_VALUE.getType();
  }
}
