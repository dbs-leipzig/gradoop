package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PropertyValueBooleanBenchmark {

    private PropertyValue BOOLEAN_VALUE;
    private PropertyValue VALUE;
    private Boolean BOOLEAN;

    @Setup
    public void setup() {
        VALUE = new PropertyValue();
        BOOLEAN_VALUE = PropertyValue.fromRawBytes(new byte[] {PropertyValue.TYPE_BOOLEAN, 0xf});
        BOOLEAN = Boolean.TRUE;
    }

    @Benchmark
    public PropertyValue create() {
        return PropertyValue.create(BOOLEAN);
    }

    @Benchmark
    public void set() {
        VALUE.setBoolean(true);
    }

    @Benchmark
    public Boolean is() {
        return BOOLEAN_VALUE.isBoolean();
    }

    @Benchmark
    public Boolean get() {
        return BOOLEAN_VALUE.getBoolean();
    }

    @Benchmark
    public void setObject() {
        VALUE.setObject(Boolean.TRUE);
    }

    @Benchmark
    public Class<?> getType() {
        return BOOLEAN_VALUE.getType();
    }
}
