package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueSetBenchmark {

    private HashSet<PropertyValue> SET;
    private PropertyValue SET_VALUE;
    private PropertyValue VALUE;

    @Setup
    public void setup() {
        VALUE = new PropertyValue();
        SET = new HashSet<>(Arrays.asList(PropertyValue.create("A"), PropertyValue.create("B"), PropertyValue.create("C")));
        SET_VALUE = PropertyValue.create(SET);
    }

    @Benchmark
    public PropertyValue create() {
        return PropertyValue.create(SET);
    }

    @Benchmark
    public void set() {
        VALUE.setSet(SET);
    }

    @Benchmark
    public Boolean is() {
        return SET_VALUE.isSet();
    }

    @Benchmark
    public Set<PropertyValue> get() {
        return SET_VALUE.getSet();
    }

    @Benchmark
    public void setObject() {
        VALUE.setObject(SET);
    }

    @Benchmark
    public Class<?> getType() {
        return SET_VALUE.getType();
    }
}
