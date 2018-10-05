package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.math.BigDecimal;

/**
 * Class for running benchmark tests concerning {@code PropertyValue}'s {@code BigDecimal}
 * representation.
 */
public class PropertyValueBigDecimalBenchmark extends AbstractPropertyValueBenchmark {

  /** PropertyValue */
  private PropertyValue VALUE;
  /** BigDecimal as PropertyValue */
  private PropertyValue BIG_DECIMAL_VALUE;
  /** BigDecimal */
  private BigDecimal BIG_DECIMAL;

  /**
   * Setup method for PropertyValueBigDecimalBenchmark
   */
  public void setup() {
    VALUE = new PropertyValue();
    BIG_DECIMAL = new BigDecimal(15);
    BIG_DECIMAL_VALUE = PropertyValue.create(BIG_DECIMAL);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(BIG_DECIMAL);
  }

  @Override
  public void set() {
    VALUE.setBigDecimal(BIG_DECIMAL);
  }

  @Override
  public Boolean is() {
    return BIG_DECIMAL_VALUE.isBigDecimal();
  }

  @Override
  public BigDecimal get() {
    return BIG_DECIMAL_VALUE.getBigDecimal();
  }

  @Override
  public void setObject() {
    VALUE.setObject(BIG_DECIMAL);
  }

  @Override
  public Class<?> getType() {
    return BIG_DECIMAL_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueBigDecimalBenchmark
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
