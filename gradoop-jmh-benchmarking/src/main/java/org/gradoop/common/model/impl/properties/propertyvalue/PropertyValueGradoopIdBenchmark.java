package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.id.GradoopId;
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
public class PropertyValueGradoopIdBenchmark {

  private PropertyValue VALUE;
  private PropertyValue GRADOOP_ID_VALUE;
  private GradoopId GRADOOP_ID;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    GRADOOP_ID_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_GRADOOP_ID, 0xf});
    GRADOOP_ID = GradoopId.get();
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(GRADOOP_ID);
  }

  @Benchmark
  public void set() {
    VALUE.setGradoopId(GRADOOP_ID);
  }

  @Benchmark
  public Boolean is() {
    return GRADOOP_ID_VALUE.isGradoopId();
  }

  @Benchmark
  public GradoopId get() {
    return GRADOOP_ID_VALUE.getGradoopId();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(GRADOOP_ID);
  }

  @Benchmark
  public Class<?> getType() {
    return GRADOOP_ID_VALUE.getType();
  }
}
