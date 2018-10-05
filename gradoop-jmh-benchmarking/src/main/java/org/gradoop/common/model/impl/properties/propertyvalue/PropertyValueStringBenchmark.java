package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueStringBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue STRING_VALUE;
  private String STRING;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    STRING = "15";
    STRING_VALUE = PropertyValue.create(STRING);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(STRING);
  }

  @Override
  public void set() {
    VALUE.setString(STRING);
  }

  @Override
  public Boolean is() {
    return STRING_VALUE.isString();
  }

  @Override
  public String get() {
    return STRING_VALUE.getString();
  }

  @Override
  public void setObject() {
    VALUE.setObject(STRING);
  }

  @Override
  public Class<?> getType() {
    return STRING_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueStringBenchmark
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
