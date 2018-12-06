package org.gradoop.common.model.impl.properties;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface PropertyValueStrategy<T> {

  boolean write(T value, DataOutputView outputView) throws IOException;

  T read(DataInputView inputView) throws IOException;

  int compare(T value, T other);

  boolean is(Object value);

  Class<T> getType();

  T get(byte[] bytes);

  Byte getRawType();

  byte[] getRawBytes(T value);

  class PropertyValueStrategyFactory {

    public static PropertyValueStrategyFactory INSTANCE = new PropertyValueStrategyFactory();
    private final Map<Class, PropertyValueStrategy> classStrategyMap;
    private final Map<Byte, PropertyValueStrategy> byteStrategyMap;
    private final NoopPropertyValueStrategy noopPropertyValueStrategy = new NoopPropertyValueStrategy();

    private PropertyValueStrategyFactory() {
      classStrategyMap = new HashMap<>();
      classStrategyMap.put(Boolean.class, new BooleanStrategy());
      classStrategyMap.put(Set.class, new SetStrategy());
      classStrategyMap.put(Integer.class, new IntegerStrategy());
      classStrategyMap.put(Long.class, new LongStrategy());
      classStrategyMap.put(Float.class, new FloatStrategy());
      classStrategyMap.put(Double.class, new DoubleStrategy());
      classStrategyMap.put(Short.class, new ShortStrategy());
      classStrategyMap.put(BigDecimal.class, new BigDecimalStrategy());
      classStrategyMap.put(LocalDate.class, new DateStrategy());
      classStrategyMap.put(LocalTime.class, new TimeStrategy());
      classStrategyMap.put(LocalDateTime.class, new DateTimeStrategy());

      byteStrategyMap = new HashMap<>(classStrategyMap.size());
      for (PropertyValueStrategy strategy : classStrategyMap.values()) {
        byteStrategyMap.put(strategy.getRawType(), strategy);
      }
    }

    public static PropertyValueStrategy get(Class c) {
      PropertyValueStrategy strategy = INSTANCE.classStrategyMap.get(c);
      if (strategy == null) {
        for (Map.Entry<Class, PropertyValueStrategy> entry : INSTANCE.classStrategyMap.entrySet()) {
          if (entry.getKey().isAssignableFrom(c)) {
            strategy = entry.getValue();
            INSTANCE.classStrategyMap.put(c, strategy);
            break;
          }
        }
      }
      return strategy == null ? INSTANCE.noopPropertyValueStrategy : strategy;
    }

    public static Object fromRawBytes(byte[] bytes) {
      PropertyValueStrategy strategy = INSTANCE.byteStrategyMap.get(bytes[0]);
      return strategy == null ? null : strategy.get(bytes);
    }

    public static int compare(Object value, Object other) {
      if (value != null) {
        PropertyValueStrategy strategy = get(value.getClass());
        if (strategy.is(other)) {
          return strategy.compare(value, other);
        }
      }
      return 0;
    }

    public static byte[] getRawBytes(Object value) {
      if (value != null) {
        return get(value.getClass()).getRawBytes(value);
      }
      return new byte[0];
    }

    public static PropertyValueStrategy get(byte value) {
      return INSTANCE.byteStrategyMap.get(value);
    }

    public static PropertyValueStrategy get(Object value) {
      if (value != null) {
        return get(value.getClass());
      }
      return INSTANCE.noopPropertyValueStrategy;
    }
  }

  class BooleanStrategy implements PropertyValueStrategy<Boolean> {

    @Override
    public boolean write(Boolean value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public Boolean read(DataInputView inputView) throws IOException {
      return inputView.readByte() == -1;
    }

    @Override
    public int compare(Boolean value, Boolean other) {
      return Boolean.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Boolean;
    }

    @Override
    public Class<Boolean> getType() {
      return Boolean.class;
    }

    @Override
    public Boolean get(byte[] bytes) {
      return bytes[1] == -1;
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_BOOLEAN;
    }

    @Override
    public byte[] getRawBytes(Boolean value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_BOOLEAN];
      rawBytes[0] = getRawType();
      Bytes.putByte(rawBytes, PropertyValue.OFFSET, (byte) ((boolean) value ? -1 : 0));
      return rawBytes;
    }
  }

  class SetStrategy implements PropertyValueStrategy<Set> {

    @Override
    public boolean write(Set value, DataOutputView outputView) throws IOException {
      byte[] rawBytes = getRawBytes(value);
      outputView.write(rawBytes);
      return true;
    }

    @Override
    public Set read(DataInputView inputView) throws IOException {
      int length = inputView.readShort();
      // init new array
      byte[] rawBytes = new byte[length];

      inputView.read(rawBytes);

      PropertyValue entry;

      Set<PropertyValue> set = new HashSet<PropertyValue>();

      ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
      DataInputStream inputStream = new DataInputStream(byteStream);
      DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);

      try {
        while (inputStream.available() > 0) {
          entry = new PropertyValue();
          entry.read(internalInputView);

          set.add(entry);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading PropertyValue");
      }

      return set;
    }

    @Override
    public int compare(Set value, Set other) {
      throw new UnsupportedOperationException("Method compareTo() is not supported");
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Set;
    }

    @Override
    public Class<Set> getType() {
      return Set.class;
    }

    @Override
    public Set get(byte[] bytes) {
      PropertyValue entry;

      Set<PropertyValue> set = new HashSet<PropertyValue>();

      ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
      DataInputStream inputStream = new DataInputStream(byteStream);
      DataInputView internalInputView = new DataInputViewStreamWrapper(inputStream);


      try {
        internalInputView.skipBytesToRead(1);
        while (inputStream.available() > 0) {
          entry = new PropertyValue();
          entry.read(internalInputView);

          set.add(entry);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading PropertyValue");
      }

      return set;
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_SET;
    }

    @Override
    public byte[] getRawBytes(Set value) {
      Set<PropertyValue> set = value;

      int size = set.stream().mapToInt(PropertyValue::byteSize).sum() + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT;

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

      try {
        outputStream.write(PropertyValue.TYPE_SET);
        outputStream.writeShort(size);
        for (PropertyValue entry : set) {
          entry.write(outputView);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error writing PropertyValue");
      }

      return byteStream.toByteArray();
    }
  }

  class IntegerStrategy implements PropertyValueStrategy<Integer> {

    @Override
    public boolean write(Integer value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public Integer read(DataInputView inputView) throws IOException {
      int length = Bytes.SIZEOF_INT;
      byte[] rawBytes = new byte[length];
      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toInt(rawBytes);
    }

    @Override
    public int compare(Integer value, Integer other) {
      return Integer.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Integer;
    }

    @Override
    public Class<Integer> getType() {
      return Integer.class;
    }

    @Override
    public Integer get(byte[] bytes) {
      return Bytes.toInt(bytes, PropertyValue.OFFSET);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_INTEGER;
    }

    @Override
    public byte[] getRawBytes(Integer value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_INT];
      rawBytes[0] = getRawType();
      Bytes.putInt(rawBytes, PropertyValue.OFFSET, value);
      return rawBytes;
    }
  }

  class LongStrategy implements PropertyValueStrategy<Long> {

    @Override
    public boolean write(Long value, DataOutputView outputView) throws IOException {
      byte[] rawBytes = getRawBytes(value);
      outputView.write(rawBytes);
      return true;
    }

    @Override
    public Long read(DataInputView inputView) throws IOException {
      int length = Bytes.SIZEOF_LONG;
      byte[] rawBytes = new byte[length];
      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toLong(rawBytes);
    }

    @Override
    public int compare(Long value, Long other) {
      return Long.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Long;
    }

    @Override
    public Class<Long> getType() {
      return Long.class;
    }

    @Override
    public Long get(byte[] bytes) {
      return Bytes.toLong(bytes, PropertyValue.OFFSET);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_LONG;
    }

    @Override
    public byte[] getRawBytes(Long value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_LONG];
      rawBytes[0] = getRawType();
      Bytes.putLong(rawBytes, PropertyValue.OFFSET, value);
      return rawBytes;
    }
  }

  class FloatStrategy implements PropertyValueStrategy<Float> {

    @Override
    public boolean write(Float value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public Float read(DataInputView inputView) throws IOException {
      int length = Bytes.SIZEOF_FLOAT;
      byte[] rawBytes = new byte[length];
      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toFloat(rawBytes);
    }

    @Override
    public int compare(Float value, Float other) {
      return Float.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Float;
    }

    @Override
    public Class<Float> getType() {
      return Float.class;
    }

    @Override
    public Float get(byte[] bytes) {
      return Bytes.toFloat(bytes, PropertyValue.OFFSET);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_FLOAT;
    }

    @Override
    public byte[] getRawBytes(Float value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_FLOAT];
      rawBytes[0] = getRawType();
      Bytes.putFloat(rawBytes, PropertyValue.OFFSET, value);
      return rawBytes;
    }
  }

  class DoubleStrategy implements PropertyValueStrategy<Double> {

    @Override
    public boolean write(Double value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public Double read(DataInputView inputView) throws IOException {
      int length = Bytes.SIZEOF_DOUBLE;
      byte[] rawBytes = new byte[length];
      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toDouble(rawBytes);
    }

    @Override
    public int compare(Double value, Double other) {
      return Double.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Double;
    }

    @Override
    public Class<Double> getType() {
      return Double.class;
    }

    @Override
    public Double get(byte[] bytes) {
      return Bytes.toDouble(bytes);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_DOUBLE;
    }

    @Override
    public byte[] getRawBytes(Double value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_DOUBLE];
      rawBytes[0] = getRawType();
      Bytes.putDouble(rawBytes, PropertyValue.OFFSET, value);
      return rawBytes;
    }
  }

  class ShortStrategy implements PropertyValueStrategy<Short> {

    @Override
    public boolean write(Short value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public Short read(DataInputView inputView) throws IOException {
      int length = Bytes.SIZEOF_SHORT;
      byte[] rawBytes = new byte[length];

      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toShort(rawBytes);
    }

    @Override
    public int compare(Short value, Short other) {
      return Short.compare(value, other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof Short;
    }

    @Override
    public Class<Short> getType() {
      return Short.class;
    }

    @Override
    public Short get(byte[] bytes) {
      return Bytes.toShort(bytes, PropertyValue.OFFSET);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_SHORT;
    }

    @Override
    public byte[] getRawBytes(Short value) {
      byte[] rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_SHORT];
      rawBytes[0] = getRawType();
      Bytes.putShort(rawBytes, PropertyValue.OFFSET, value);
      return rawBytes;
    }
  }

  class BigDecimalStrategy implements PropertyValueStrategy<BigDecimal> {

    @Override
    public boolean write(BigDecimal value, DataOutputView outputView) throws IOException {
      byte[] rawBytes = getRawBytes(value);
      byte type = rawBytes[0];

      if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
        type |= PropertyValue.FLAG_LARGE;
      }
      outputView.writeByte(type);
      // Write length as an int if the "large" flag is set.
      if ((type & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
        outputView.writeInt(rawBytes.length - PropertyValue.OFFSET);
      } else {
        outputView.writeShort(rawBytes.length - PropertyValue.OFFSET);
      }

      outputView.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
      return true;
    }

    @Override
    public BigDecimal read(DataInputView inputView) throws IOException {
      // @TODO This will break as soon as BigDecimal gets real big, find a whether readInt needs to be used
      int length = inputView.readShort();
      byte[] rawBytes = new byte[length];
      for (int i = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
      return Bytes.toBigDecimal(rawBytes);
    }

    @Override
    public int compare(BigDecimal value, BigDecimal other) {
      return value.compareTo(other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof BigDecimal;
    }

    @Override
    public Class<BigDecimal> getType() {
      return BigDecimal.class;
    }

    @Override
    public BigDecimal get(byte[] bytes) {
      return Bytes.toBigDecimal(bytes, PropertyValue.OFFSET, bytes.length - PropertyValue.OFFSET);
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_BIG_DECIMAL;
    }

    @Override
    public byte[] getRawBytes(BigDecimal value) {
      byte[] valueBytes = Bytes.toBytes(value);
      byte[] rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
      rawBytes[0] = getRawType();
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
      return rawBytes;
    }
  }

  class DateStrategy implements PropertyValueStrategy <LocalDate> {

    @Override
    public boolean write(LocalDate value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public LocalDate read(DataInputView inputView) throws IOException {
      int length = DateTimeSerializer.SIZEOF_DATE;
      byte[] rawBytes = new byte[length];

      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }

      return DateTimeSerializer.deserializeDate(rawBytes);
    }

    @Override
    public int compare(LocalDate value, LocalDate other) {
      return value.compareTo(other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof LocalDate;
    }

    @Override
    public Class<LocalDate> getType() {
      return LocalDate.class;
    }

    @Override
    public LocalDate get(byte[] bytes) {
      return DateTimeSerializer.deserializeDate(
        Arrays.copyOfRange(
          bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATE + PropertyValue.OFFSET
        ));
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_DATE;
    }

    @Override
    public byte[] getRawBytes(LocalDate value) {
      byte[] valueBytes = DateTimeSerializer.serializeDate(value);
      byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATE];
      rawBytes[0] = getRawType();
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
      return rawBytes;
    }
  }

  class TimeStrategy implements PropertyValueStrategy<LocalTime> {

    @Override
    public boolean write(LocalTime value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public LocalTime read(DataInputView inputView) throws IOException {
      int length = DateTimeSerializer.SIZEOF_TIME;
      byte[] rawBytes = new byte[length];

      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }

      return DateTimeSerializer.deserializeTime(rawBytes);
    }

    @Override
    public int compare(LocalTime value, LocalTime other) {
      return value.compareTo(other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof LocalTime;
    }

    @Override
    public Class<LocalTime> getType() {
      return LocalTime.class;
    }

    @Override
    public LocalTime get(byte[] bytes) {
      return DateTimeSerializer.deserializeTime(
        Arrays.copyOfRange(
          bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_TIME + PropertyValue.OFFSET
        ));
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_TIME;
    }

    @Override
    public byte[] getRawBytes(LocalTime value) {
      byte[] valueBytes = DateTimeSerializer.serializeTime(value);
      byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_TIME];
      rawBytes[0] = getRawType();
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
      return rawBytes;
    }
  }

  class DateTimeStrategy implements PropertyValueStrategy<LocalDateTime> {

    @Override
    public boolean write(LocalDateTime value, DataOutputView outputView) throws IOException {
      outputView.write(getRawBytes(value));
      return true;
    }

    @Override
    public LocalDateTime read(DataInputView inputView) throws IOException {
      int length = DateTimeSerializer.SIZEOF_DATETIME;
      byte[] rawBytes = new byte[length];

      for (int i  = 0; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }

      return DateTimeSerializer.deserializeDateTime(rawBytes);
    }

    @Override
    public int compare(LocalDateTime value, LocalDateTime other) {
      return value.compareTo(other);
    }

    @Override
    public boolean is(Object value) {
      return value instanceof LocalDateTime;
    }

    @Override
    public Class<LocalDateTime> getType() {
      return LocalDateTime.class;
    }

    @Override
    public LocalDateTime get(byte[] bytes) {
      return DateTimeSerializer.deserializeDateTime(
        Arrays.copyOfRange(
          bytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATETIME + PropertyValue.OFFSET
        ));
    }

    @Override
    public Byte getRawType() {
      return PropertyValue.TYPE_DATETIME;
    }

    @Override
    public byte[] getRawBytes(LocalDateTime value) {
      byte[] valueBytes = DateTimeSerializer.serializeDateTime(value);
      byte[] rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATETIME];
      rawBytes[0] = getRawType();
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
      return rawBytes;
    }
  }

  class NoopPropertyValueStrategy implements PropertyValueStrategy {
    @Override
    public boolean write(Object value, DataOutputView outputView) {
      return false;
    }

    @Override
    public Object read(DataInputView inputView) throws IOException {
      return null;
    }

    @Override
    public int compare(Object value, Object other) {
      return 0;
    }

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
}
