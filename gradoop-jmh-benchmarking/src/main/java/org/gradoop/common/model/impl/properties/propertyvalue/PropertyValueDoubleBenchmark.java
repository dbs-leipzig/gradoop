package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueDoubleBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue DOUBLE_VALUE;
  private Double DOUBLE;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    DOUBLE = 1.5;
    DOUBLE_VALUE = PropertyValue.create(DOUBLE);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(1.5);
  }

  @Override
  public void set() {
    VALUE.setDouble(DOUBLE);
  }

  @Override
  public Boolean is() {
    return DOUBLE_VALUE.isDouble();
  }

  @Override
  public Double get() {
    return DOUBLE_VALUE.getDouble();
  }

  @Override
  public void setObject() {
    VALUE.setObject(DOUBLE);
  }

  @Override
  public Class<?> getType() {
    return DOUBLE_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueDoubleBenchmark
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