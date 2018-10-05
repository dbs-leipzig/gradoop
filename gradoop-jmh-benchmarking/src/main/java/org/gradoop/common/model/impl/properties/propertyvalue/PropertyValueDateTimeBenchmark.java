package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueDateTimeBenchmark {

  private LocalDateTime DATETIME;
  private PropertyValue DATETIME_VALUE;
  private PropertyValue VALUE;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    DATETIME = LocalDateTime.now();
    DATETIME_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_DATETIME, 0xf});
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(DATETIME);
  }

  @Benchmark
  public void set() {
    VALUE.setDateTime(DATETIME);
  }

  @Benchmark
  public Boolean is() {
    return DATETIME_VALUE.isDateTime();
  }

  @Benchmark
  public LocalDateTime get() {
    return DATETIME_VALUE.getDateTime();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(DATETIME);
  }

  @Benchmark
  public Class<?> getType() {
    return DATETIME_VALUE.getType();
  }
}