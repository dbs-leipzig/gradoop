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
public class PropertyValueShortBenchmark {

  private PropertyValue VALUE;
  private PropertyValue SHORT_VALUE;
  private Short SHORT;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    SHORT_VALUE = PropertyValue.fromRawBytes(new byte[] {0x0e, 0xf});
    SHORT = (short) 15;
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create((short) 1);
  }

  @Benchmark
  public void set() {
    VALUE.setShort(SHORT);
  }

  @Benchmark
  public Boolean is() {
    return SHORT_VALUE.isShort();
  }

  @Benchmark
  public Short get() {
    return SHORT_VALUE.getShort();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(SHORT);
  }

  @Benchmark
  public Class<?> getType() {
    return SHORT_VALUE.getType();
  }
}
