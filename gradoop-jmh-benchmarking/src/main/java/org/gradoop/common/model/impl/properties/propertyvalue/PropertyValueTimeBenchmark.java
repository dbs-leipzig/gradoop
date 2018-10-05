package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.time.LocalTime;

public class PropertyValueTimeBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue TIME_VALUE;
  private LocalTime TIME;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    TIME = LocalTime.now();
    TIME_VALUE = PropertyValue.create(TIME);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(TIME);
  }

  @Override
  public void set() {
    VALUE.setTime(TIME);
  }

  @Override
  public Boolean is() {
    return TIME_VALUE.isTime();
  }

  @Override
  public LocalTime get() {
    return TIME_VALUE.getTime();
  }

  @Override
  public void setObject() {
    VALUE.setObject(TIME);
  }

  @Override
  public Class<?> getType() {
    return TIME_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueTimeBenchmark
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
