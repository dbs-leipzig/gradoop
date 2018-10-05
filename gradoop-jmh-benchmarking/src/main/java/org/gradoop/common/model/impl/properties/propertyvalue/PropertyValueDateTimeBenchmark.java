package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.time.LocalDateTime;

public class PropertyValueDateTimeBenchmark extends AbstractPropertyValueBenchmark {

  private LocalDateTime DATETIME;
  private PropertyValue DATETIME_VALUE;
  private PropertyValue VALUE;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    DATETIME = LocalDateTime.now();
    DATETIME_VALUE = PropertyValue.create(DATETIME);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(DATETIME);
  }

  @Override
  public void set() {
    VALUE.setDateTime(DATETIME);
  }

  @Override
  public Boolean is() {
    return DATETIME_VALUE.isDateTime();
  }

  @Override
  public LocalDateTime get() {
    return DATETIME_VALUE.getDateTime();
  }

  @Override
  public void setObject() {
    VALUE.setObject(DATETIME);
  }

  @Override
  public Class<?> getType() {
    return DATETIME_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueDateTimeBenchmark
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