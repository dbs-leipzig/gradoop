package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PropertyValueListBenchmark extends AbstractPropertyValueBenchmark {

  private List<PropertyValue> LIST;
  private PropertyValue LIST_VALUE;
  private PropertyValue VALUE;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    LIST = new ArrayList<>(
      Arrays.asList(PropertyValue.create("A"), PropertyValue.create("B"), PropertyValue.create("C"))
    );
    LIST_VALUE = PropertyValue.create(LIST);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(LIST);
  }

  @Override
  public void set() {
    VALUE.setList(LIST);
  }

  @Override
  public Boolean is() {
    return LIST_VALUE.isList();
  }

  @Override
  public List<PropertyValue> get() {
    return LIST_VALUE.getList();
  }

  @Override
  public void setObject() {
    VALUE.setObject(LIST);
  }

  @Override
  public Class<?> getType() {
    return LIST_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueListBenchmark
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
