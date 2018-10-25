package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public interface PropertyValueStrategy {

    class PropertyValueStrategyFactory {

        public static PropertyValueStrategyFactory INSTANCE =  new PropertyValueStrategyFactory();

        public static PropertyValueStrategy get(Class c) {
            PropertyValueStrategy strategy = INSTANCE.strategyMap.get(c);
            return strategy == null ? INSTANCE.noopPropertyValueStrategy : strategy ;
        }
        private final Map<Class, PropertyValueStrategy> strategyMap;

        private final NoopPropertyValueStrategy noopPropertyValueStrategy = new NoopPropertyValueStrategy();

        private PropertyValueStrategyFactory() {
            strategyMap = new HashMap<>();
            strategyMap.put(Boolean.class, new BooleanStrategy());
        }

    }
    class BooleanStrategy implements PropertyValueStrategy {
        @Override
        public boolean is(Object value) {
            return value instanceof Boolean;
        }

        @Override
        public Class<?> getType() {
            return Boolean.class;
        }

        @Override
        public byte[] getRawBytes(Object value) {
            byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_BOOLEAN];
            rawBytes[0] = PropertyValue.TYPE_BOOLEAN;
            Bytes.putByte(rawBytes, PropertyValue.OFFSET, (byte) ((boolean)value ? -1 : 0));
            return rawBytes;
        }


    }
    class NoopPropertyValueStrategy implements PropertyValueStrategy {
        @Override
        public boolean is(Object value) {
            return false;
        }

        @Override
        public Class<?> getType() {
            return null;
        }

        @Override
        public byte[] getRawBytes(Object value) {
            return null;
        }

    }

    boolean is(Object value);
    Class<?> getType();
    byte[] getRawBytes(Object value);
}
