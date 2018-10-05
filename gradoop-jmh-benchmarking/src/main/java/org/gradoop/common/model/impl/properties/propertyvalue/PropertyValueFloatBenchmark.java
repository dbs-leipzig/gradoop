package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueFloatBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue FLOAT_VALUE;
  private Float FLOAT;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    FLOAT = (float) 1.5;
    FLOAT_VALUE = PropertyValue.create(FLOAT);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(FLOAT);
  }

  @Override
  public void set() {
    VALUE.setFloat(FLOAT);
  }

  @Override
  public Boolean is() {
    return FLOAT_VALUE.isFloat();
  }

  @Override
  public Float get() {
    return FLOAT_VALUE.getFloat();
  }

  @Override
  public void setObject() {
    VALUE.setObject(FLOAT);
  }

  @Override
  public Class<?> getType() {
    return FLOAT_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueFloatBenchmark
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