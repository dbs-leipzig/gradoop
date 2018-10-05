package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueGradoopIdBenchmark extends AbstractPropertyValueBenchmark {

  private PropertyValue VALUE;
  private PropertyValue GRADOOP_ID_VALUE;
  private GradoopId GRADOOP_ID;

  @Override
  public void setup() {
    VALUE = new PropertyValue();
    GRADOOP_ID = GradoopId.get();
    GRADOOP_ID_VALUE = PropertyValue.create(GRADOOP_ID);
  }

  @Override
  public PropertyValue create() {
    return PropertyValue.create(GRADOOP_ID);
  }

  @Override
  public void set() {
    VALUE.setGradoopId(GRADOOP_ID);
  }

  @Override
  public Boolean is() {
    return GRADOOP_ID_VALUE.isGradoopId();
  }

  @Override
  public GradoopId get() {
    return GRADOOP_ID_VALUE.getGradoopId();
  }

  @Override
  public void setObject() {
    VALUE.setObject(GRADOOP_ID);
  }

  @Override
  public Class<?> getType() {
    return GRADOOP_ID_VALUE.getType();
  }

  /**
   * You can run this test:
   *
   * a) Via the command line:
   *    $ mvn clean install
   *    $ java -jar target/benchmarks.jar PropertyValueGradoopIdBenchmark
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
