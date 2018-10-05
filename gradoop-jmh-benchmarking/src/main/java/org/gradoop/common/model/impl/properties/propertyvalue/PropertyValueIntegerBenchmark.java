package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueIntegerBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue INTEGER_VALUE;
  private Integer INTEGER;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    INTEGER = 15;
    INTEGER_VALUE = PropertyValue.create(INTEGER);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(1);
  }

  @Override
  public void set() {
    VALUE.setInt(INTEGER);
  }

  @Override
  public Boolean is() {
    return INTEGER_VALUE.isInt();
  }

  @Override
  public Integer get() {
    return INTEGER_VALUE.getInt();
  }

  @Override
  public void setObject() {
    VALUE.setObject(INTEGER);
  }

  @Override
  public Class<?> getType() {
    return INTEGER_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueIntegerBenchmark
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
