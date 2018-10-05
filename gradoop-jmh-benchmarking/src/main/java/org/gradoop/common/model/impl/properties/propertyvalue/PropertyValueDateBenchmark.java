package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueDateBenchmark {

  private PropertyValue VALUE;
  private PropertyValue DATE_VALUE;
  private LocalDate DATE;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    DATE_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_DATE, 0xf});
    DATE = LocalDate.now();
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(DATE);
  }

  @Benchmark
  public void set() {
    VALUE.setDate(DATE);
  }

  @Benchmark
  public Boolean is() {
    return DATE_VALUE.isDate();
  }

  @Benchmark
  public LocalDate get() {
    return DATE_VALUE.getDate();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(DATE);
  }

  @Benchmark
  public Class<?> getType() {
    return DATE_VALUE.getType();
  }
}
