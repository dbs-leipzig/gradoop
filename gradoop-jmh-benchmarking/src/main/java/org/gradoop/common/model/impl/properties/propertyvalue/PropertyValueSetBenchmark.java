package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.HashSet;

@Warmup(time = 1)
@Measurement(time = 1)
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
    public void create() {
        PropertyValue.create(Boolean.TRUE);
    }

    @Benchmark
    public void set() {
        VALUE.setSet(SET);
    }

    @Benchmark
    public void is() {
        SET_VALUE.isSet();
    }

    @Benchmark
    public void get() {
        SET_VALUE.getSet();
    }

    @Benchmark
    public void setObject() {
        VALUE.setObject(SET);
    }

    @Benchmark
    public void getType() {
        SET_VALUE.getType();
    }
}
