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
public class PropertyValueLongBenchmark {

  private PropertyValue VALUE;
  private PropertyValue LONG_VALUE;
  private Long LONG;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    LONG_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_LONG, 0xf});
    LONG = 15L;
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(LONG);
  }

  @Benchmark
  public void set() {
    VALUE.setLong(LONG);
  }

  @Benchmark
  public Boolean is() {
    return LONG_VALUE.isLong();
  }

  @Benchmark
  public Long get() {
    return LONG_VALUE.getLong();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(LONG);
  }

  @Benchmark
  public Class<?> getType() {
    return LONG_VALUE.getType();
  }
}
