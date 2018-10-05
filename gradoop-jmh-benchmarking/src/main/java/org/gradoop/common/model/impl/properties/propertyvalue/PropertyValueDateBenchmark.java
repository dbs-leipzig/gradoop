package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.time.LocalDate;

public class PropertyValueDateBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue DATE_VALUE;
  private LocalDate DATE;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    DATE = LocalDate.now();
    DATE_VALUE = PropertyValue.create(DATE);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(DATE);
  }

  @Override
  public void set() {
    VALUE.setDate(DATE);
  }

  @Override
  public Boolean is() {
    return DATE_VALUE.isDate();
  }

  @Override
  public LocalDate get() {
    return DATE_VALUE.getDate();
  }

  @Override
  public void setObject() {
    VALUE.setObject(DATE);
  }

  @Override
  public Class<?> getType() {
    return DATE_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueDateBenchmark
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
