package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueListBenchmark {

  private List<PropertyValue> LIST;
  private PropertyValue LIST_VALUE;
  private PropertyValue VALUE;

  @Setup
  public void setup() {
    VALUE = new PropertyValue();
    LIST = new ArrayList<>(Arrays.asList(PropertyValue.create("A"), PropertyValue.create("B"), PropertyValue.create("C")));
    LIST_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_LIST, 0xf});
  }

  @Benchmark
  public PropertyValue create() {
    return PropertyValue.create(LIST);
  }

  @Benchmark
  public void set() {
    VALUE.setList(LIST);
  }

  @Benchmark
  public Boolean is() {
    return LIST_VALUE.isList();
  }

  @Benchmark
  public List<PropertyValue> get() {
    return LIST_VALUE.getList();
  }

  @Benchmark
  public void setObject() {
    VALUE.setObject(LIST);
  }

  @Benchmark
  public Class<?> getType() {
    return LIST_VALUE.getType();
  }
}
