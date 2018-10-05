package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueTimeBenchmark {

  private PropertyValue VALUE;
  private PropertyValue TIME_VALUE;
  private LocalTime TIME;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    TIME_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_TIME, 0xf});
    TIME = LocalTime.now();
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(TIME);
  }

  @Benchmark
  public void set() {
    VALUE.setTime(TIME);
  }

  @Benchmark
  public Boolean is() {
    return TIME_VALUE.isTime();
  }

  @Benchmark
  public LocalTime get() {
    return TIME_VALUE.getTime();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(TIME);
  }

  @Benchmark
  public Class<?> getType() {
    return TIME_VALUE.getType();
  }
}
