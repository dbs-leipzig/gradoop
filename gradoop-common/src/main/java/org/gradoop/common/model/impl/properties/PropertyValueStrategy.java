package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public interface PropertyValueStrategy {

    class PropertyValueStrategyFactory {

        public static PropertyValueStrategyFactory INSTANCE =  new PropertyValueStrategyFactory();

        public static PropertyValueStrategy get(Class c) {
            PropertyValueStrategy strategy = INSTANCE.classStrategyMap.get(c);
            return strategy == null ? INSTANCE.noopPropertyValueStrategy : strategy ;
        }
        public static Object fromRawBytes(byte[] bytes) {
            PropertyValueStrategy strategy = INSTANCE.byteStrategyMap.get(bytes[0]);
            return strategy == null ? null : strategy.get(bytes);
        }
        private final Map<Class, PropertyValueStrategy> classStrategyMap;

        private final Map<Byte, PropertyValueStrategy> byteStrategyMap;

        private final NoopPropertyValueStrategy noopPropertyValueStrategy = new NoopPropertyValueStrategy();

        private PropertyValueStrategyFactory() {
            classStrategyMap = new HashMap<>();
            classStrategyMap.put(Boolean.class, new BooleanStrategy());

            byteStrategyMap = new HashMap<>(classStrategyMap.size());
            for (PropertyValueStrategy strategy : classStrategyMap.values()) {
                byteStrategyMap.put(strategy.getRawType(), strategy);
            }
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
        public Object get(byte[] bytes) {
            return bytes[1] == -1;
        }

        @Override
        public Byte getRawType() {
            return PropertyValue.TYPE_BOOLEAN;
        }

        @Override
        public byte[] getRawBytes(Object value) {
            byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_BOOLEAN];
            rawBytes[0] = getRawType();
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
        public Object get(byte[] bytes) {
            return null;
        }

        @Override
        public Byte getRawType() {
            return null;
        }

        @Override
        public byte[] getRawBytes(Object value) {
            return null;
        }


    }
    boolean is(Object value);
    Class<?> getType();
    Object get(byte[] bytes);

    Byte getRawType();
    byte[] getRawBytes(Object value);
}
