package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.HashMap;
import java.util.Map;

public class PropertyValueMapBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue MAP_VALUE;
  private Map<PropertyValue, PropertyValue> MAP;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    MAP = new HashMap<>();
    MAP.put(PropertyValue.create("A"), PropertyValue.create("B"));
    MAP_VALUE = PropertyValue.create(MAP);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(MAP);
  }

  @Override
  public void set() {
    VALUE.setMap(MAP);
  }

  @Override
  public Boolean is() {
    return MAP_VALUE.isMap();
  }

  @Override
  public Map<PropertyValue, PropertyValue> get() {
    return MAP_VALUE.getMap();
  }

  @Override
  public void setObject() {
    VALUE.setObject(MAP);
  }

  @Override
  public Class<?> getType() {
    return MAP_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueMapBenchmark
   *
   * b) Via the Java API:
   *    (see the JMH homepage for possible caveats when running from IDE:
   *     http://openjdk.java.net/projects/code-tools/jmh/)
   *
   * If you want to write the benchmark results to a csv file, add -rff <filename>.csv to the
   * program call.
   *
   * @param args Program arguments
   * @throws RunnerException when something went wrong
   */
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(PropertyValueBooleanBenchmark.class.getSimpleName())
      .build();

    new Runner(opt).run();
  }
}
