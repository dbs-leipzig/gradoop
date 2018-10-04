package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueBigDecimalBenchmark {

  private PropertyValue VALUE;
  private PropertyValue BIG_DECIMAL_VALUE;
  private BigDecimal BIG_DECIMAL;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    BIG_DECIMAL_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_BIG_DECIMAL,
      BIG_DECIMAL.byteValue()});
    BIG_DECIMAL = new BigDecimal(15);
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(BIG_DECIMAL);
  }

  @Benchmark
  public void set() {
    VALUE.setBigDecimal(BIG_DECIMAL);
  }

  @Benchmark
  public Boolean is() {
    return BIG_DECIMAL_VALUE.isBigDecimal();
  }

  @Benchmark
  public BigDecimal get() {
    return BIG_DECIMAL_VALUE.getBigDecimal();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(BIG_DECIMAL);
  }

  @Benchmark
  public Class<?> getType() {
    return BIG_DECIMAL_VALUE.getType();
  }
}