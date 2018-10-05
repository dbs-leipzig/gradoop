package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PropertyValueSetBenchmark extends AbstractPropertyValueBenchmark {

    private HashSet<PropertyValue> SET;
    private PropertyValue SET_VALUE;
    private PropertyValue VALUE;

    @Override
    public void setup() {
        VALUE = new PropertyValue();
        SET = new HashSet<>(Arrays.asList(PropertyValue.create("A"), PropertyValue.create("B"), PropertyValue.create("C")));
        SET_VALUE = PropertyValue.create(SET);
    }

    @Override
    public PropertyValue create() {
        return PropertyValue.create(SET);
    }

    @Override
    public void set() {
        VALUE.setSet(SET);
    }

    @Override
    public Boolean is() {
        return SET_VALUE.isSet();
    }

    @Override
    public Set<PropertyValue> get() {
        return SET_VALUE.getSet();
    }

    @Override
    public void setObject() {
        VALUE.setObject(SET);
    }

    @Override
    public Class<?> getType() {
        return SET_VALUE.getType();
    }

    /**
     * You can run this test:
     *
     * a) Via the command line:
     *    $ mvn clean install
     *    $ java -jar target/benchmarks.jar PropertyValueSetBenchmark
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
