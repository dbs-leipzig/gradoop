package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueLongBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue LONG_VALUE;
  private Long LONG;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    LONG = 15L;
    LONG_VALUE = PropertyValue.create(LONG);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(LONG);
  }

  @Override
  public void set() {
    VALUE.setLong(LONG);
  }

  @Override
  public Boolean is() {
    return LONG_VALUE.isLong();
  }

  @Override
  public Long get() {
    return LONG_VALUE.getLong();
  }

  @Override
  public void setObject() {
    VALUE.setObject(LONG);
  }

  @Override
  public Class<?> getType() {
    return LONG_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueLongBenchmark
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
