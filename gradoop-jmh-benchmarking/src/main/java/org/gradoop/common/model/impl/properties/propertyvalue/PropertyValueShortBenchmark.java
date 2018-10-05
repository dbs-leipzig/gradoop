package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class PropertyValueShortBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue SHORT_VALUE;
  private Short SHORT;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    SHORT = (short) 15;
    SHORT_VALUE = PropertyValue.create(SHORT);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create((short) 1);
  }

  @Override
  public void set() {
    VALUE.setShort(SHORT);
  }

  @Override
  public Boolean is() {
    return SHORT_VALUE.isShort();
  }

  @Override
  public Short get() {
    return SHORT_VALUE.getShort();
  }

  @Override
  public void setObject() {
    VALUE.setObject(SHORT);
  }

  @Override
  public Class<?> getType() {
    return SHORT_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueShortBenchmark
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
