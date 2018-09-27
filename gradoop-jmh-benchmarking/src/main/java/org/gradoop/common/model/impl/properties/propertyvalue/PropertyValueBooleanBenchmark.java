package org.gradoop.common.model.impl.properties.propertyvalue;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(time = 1)
@Measurement(time = 1)
public class PropertyValueBooleanBenchmark {

    private static byte[] RAW_BYTES = new byte[] {0x1, 0xf};
    private static PropertyValue BOOLEAN_VALUE = PropertyValue.fromRawBytes(RAW_BYTES);
    private static PropertyValue VALUE = new PropertyValue();

    @Benchmark
    public void create() {
        PropertyValue.create(Boolean.TRUE);
    }

    @Benchmark
    public void setBoolean() {
        VALUE.setBoolean(true);
    }

    @Benchmark
    public void isBoolean() {
        BOOLEAN_VALUE.isBoolean();
    }

    @Benchmark
    public void getBoolean() {
        BOOLEAN_VALUE.getBoolean();
    }

    @Benchmark
    public void setObject() {
        VALUE.setObject(Boolean.TRUE);
    }

    @Benchmark
    public void getType() {
        BOOLEAN_VALUE.getType();
    }
}
