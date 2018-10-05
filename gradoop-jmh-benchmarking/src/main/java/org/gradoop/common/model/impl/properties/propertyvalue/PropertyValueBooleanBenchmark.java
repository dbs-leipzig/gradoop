package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class PropertyValueBooleanBenchmark extends AbstractPropertyValueBenchmark {

    private PropertyValue BOOLEAN_VALUE;
    private PropertyValue VALUE;
    private Boolean BOOLEAN;

    @Override
    public void setup() {
        VALUE = new PropertyValue();
        BOOLEAN_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_BOOLEAN, 0xf});
        BOOLEAN = Boolean.TRUE;
    }

    @Override
    public PropertyValue create() {
        return PropertyValue.create(BOOLEAN);
    }

    @Override
    public void set() {
        VALUE.setBoolean(true);
    }

    @Override
    public Boolean is() {
        return BOOLEAN_VALUE.isBoolean();
    }

    @Override
    public Boolean get() {
        return BOOLEAN_VALUE.getBoolean();
    }

    @Override
    public void setObject() {
        VALUE.setObject(Boolean.TRUE);
    }

    @Override
    public Class<?> getType() {
        return BOOLEAN_VALUE.getType();
    }

    /**
     * You can run this test:
     *
     * a) Via the command line:
     *    $ mvn clean install
     *    $ java -jar target/benchmarks.jar PropertyValueBooleanBenchmark
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
