/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.model.impl.properties;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.Value;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.gradoop.common.util.GradoopConstants;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single property value in the EPGM.
 *
 * A property value wraps a value that implements a supported data type.
 */
public class PropertyValue implements Value, Serializable, Comparable<PropertyValue> {

  /**
   * Represents a property value that is {@code null}.
   */
  public static final PropertyValue NULL_VALUE = PropertyValue.create(null);

  /**
   * {@code <property-type>} for empty property value (i.e. {@code null})
   */
  public static final transient byte TYPE_NULL         = 0x00;
  /**
   * {@code <property-type>} for {@link java.lang.Boolean}
   */
  public static final transient byte TYPE_BOOLEAN      = 0x01;
  /**
   * {@code <property-type>} for {@link java.lang.Integer}
   */
  public static final transient byte TYPE_INTEGER      = 0x02;
  /**
   * {@code <property-type>} for {@link java.lang.Long}
   */
  public static final transient byte TYPE_LONG         = 0x03;
  /**
   * {@code <property-type>} for {@link java.lang.Float}
   */
  public static final transient byte TYPE_FLOAT        = 0x04;
  /**
   * {@code <property-type>} for {@link java.lang.Double}
   */
  public static final transient byte TYPE_DOUBLE       = 0x05;
  /**
   * {@code <property-type>} for {@link java.lang.String}
   */
  public static final transient byte TYPE_STRING       = 0x06;
  /**
   * {@code <property-type>} for {@link java.lang.String}
   */
  public static final transient byte TYPE_BIG_DECIMAL  = 0x07;
  /**
   * {@code <property-type>} for {@link org.gradoop.common.model.impl.id.GradoopId}
   */
  public static final transient byte TYPE_GRADOOP_ID   = 0x08;
  /**
   * {@code <property-type>} for {@link java.util.HashMap}
   */
  public static final transient byte TYPE_MAP          = 0x09;
  /**
   * {@code <property-type>} for {@link java.util.List}
   */
  public static final transient byte TYPE_LIST         = 0x0a;
  /**
   * {@code <property-type>} for {@link java.util.List}
   */
  public static final transient byte TYPE_DATE         = 0x0b;
  /**
   * {@code <property-type>} for {@link java.util.List}
   */
  public static final transient byte TYPE_TIME         = 0x0c;
  /**
   * {@code <property-type>} for {@link java.util.List}
   */
  public static final transient byte TYPE_DATETIME     = 0x0d;

  /**
   * Value offset in byte
   */
  public static final transient byte OFFSET            = 0x01;

  /**
   * We use a short as length prefix in binary representations of this property.
   * This value is the maximum viable length.
   */
  public static final int MAX_BINARY_LENGTH = Short.MAX_VALUE - OFFSET;

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Stores the type and the value
    */
  private byte[] rawBytes;

  /**
   * Default constructor.
   */
  public PropertyValue() { }

  /**
   * Creates a new property value from the given value.
   *
   * If the given object type is not supported, an
   * {@link UnsupportedTypeException} will be thrown.
   *
   * @param value value with supported type
   */
  private PropertyValue(Object value) {
    setObject(value);
  }

  /**
   * Creates a new property value from the given byte array.
   *
   * @param bytes byte array
   */
  private PropertyValue(byte[] bytes) {
    rawBytes = bytes;
  }

  /**
   * Creates a new Property Value from the given object.
   *
   * If the given object type is not supported, an
   * {@link UnsupportedTypeException} will be thrown.
   *
   * @param value value with supported type
   * @return property value
   */
  public static PropertyValue create(Object value) {
    return new PropertyValue(value);
  }

  //----------------------------------------------------------------------------
  // Type checking
  //----------------------------------------------------------------------------

  /**
   * True, if the value represents {@code null}.
   *
   * @return true, if {@code null} value
   */
  public boolean isNull() {
    return rawBytes[0] == TYPE_NULL;
  }

  /**
   * True, if the wrapped value is of type {@code boolean}.
   *
   * @return true, if {@code boolean} value
   */
  public boolean isBoolean() {
    return rawBytes[0] == TYPE_BOOLEAN;
  }
  /**
   * True, if the wrapped value is of type {@code int}.
   *
   * @return true, if {@code int} value
   */
  public boolean isInt() {
    return rawBytes[0] == TYPE_INTEGER;
  }
  /**
   * True, if the wrapped value is of type {@code long}.
   *
   * @return true, if {@code long} value
   */
  public boolean isLong() {
    return rawBytes[0] == TYPE_LONG;
  }
  /**
   * True, if the wrapped value is of type {@code float}.
   *
   * @return true, if {@code float} value
   */
  public boolean isFloat() {
    return rawBytes[0] == TYPE_FLOAT;
  }
  /**
   * True, if the wrapped value is of type {@code double}.
   *
   * @return true, if {@code double} value
   */
  public boolean isDouble() {
    return rawBytes[0] == TYPE_DOUBLE;
  }
  /**
   * True, if the wrapped value is of type {@code String}.
   *
   * @return true, if {@code String} value
   */
  public boolean isString() {
    return rawBytes[0] == TYPE_STRING;
  }
  /**
   * True, if the wrapped value is of type {@code BigDecimal}.
   *
   * @return true, if {@code BigDecimal} value
   * @see BigDecimal
   */
  public boolean isBigDecimal() {
    return rawBytes[0] == TYPE_BIG_DECIMAL;
  }
  /**
   * True, if the wrapped value is of type {@code GradoopId}.
   *
   * @return true, if {@code GradoopId} value
   */
  public boolean isGradoopId() {
    return rawBytes[0] == TYPE_GRADOOP_ID;
  }
  /**
   * True, if the wrapped value is of type {@code Map}.
   *
   * @return true, if {@code Map} value
   */
  public boolean isMap() {
    return rawBytes[0] == TYPE_MAP;
  }
  /**
   * True, if the wrapped value is of type {@code List}.
   *
   * @return true, if {@code List} value
   */
  public boolean isList() {
    return rawBytes[0] == TYPE_LIST;
  }
  /**
   * True, if the wrapped value is of type {@code LocalDate}.
   *
   * @return true, if {@code LocalDate} value
   */
  public boolean isDate() {
    return rawBytes[0] == TYPE_DATE;
  }
  /**
   * True, if the wrapped value is of type {@code LocalTime}.
   *
   * @return true, if {@code LocalTime} value
   */
  public boolean isTime() {
    return rawBytes[0] == TYPE_TIME;
  }
  /**
   * True, if the wrapped value is of type {@code LocalDateTime}.
   *
   * @return true, if {@code LocalDateTime} value
   */
  public boolean isDateTime() {
    return rawBytes[0] == TYPE_DATETIME;
  }

  //----------------------------------------------------------------------------
  // Getter
  //----------------------------------------------------------------------------

  /**
   * Returns the wrapped value as object.
   *
   * @return value or {@code null} if the value is empty
   */
  public Object getObject() {
    return isBoolean() ? getBoolean() :
      isInt() ? getInt() :
        isLong() ? getLong() :
          isFloat() ? getFloat() :
            isDouble() ? getDouble() :
              isString() ? getString() :
                isBigDecimal() ? getBigDecimal() :
                  isGradoopId() ? getGradoopId() :
                    isMap() ? getMap() :
                      isList() ? getList() :
                        isDate() ? getDate() :
                          isTime() ? getTime() :
                            isDateTime() ? getDateTime() :
                              null;
  }
  /**
   * Returns the wrapped value as {@code boolean}.
   *
   * @return {@code boolean} value
   */
  public boolean getBoolean() {
    return rawBytes[1] == -1;
  }
  /**
   * Returns the wrapped value as {@code int}.
   *
   * @return {@code int} value
   */
  public int getInt() {
    return Bytes.toInt(rawBytes, OFFSET);
  }
  /**
   * Returns the wrapped value as {@code long}.
   *
   * @return {@code long} value
   */
  public long getLong() {
    return Bytes.toLong(rawBytes, OFFSET);
  }
  /**
   * Returns the wrapped value as {@code float}.
   *
   * @return {@code float} value
   */
  public float getFloat() {
    return Bytes.toFloat(rawBytes, OFFSET);
  }
  /**
   * Returns the wrapped value as {@code double}.
   *
   * @return {@code double} value
   */
  public double getDouble() {
    return Bytes.toDouble(rawBytes, OFFSET);
  }
  /**
   * Returns the wrapped value as {@code String}.
   *
   * @return {@code String} value
   */
  public String getString() {
    return Bytes.toString(rawBytes, OFFSET, rawBytes.length - OFFSET);
  }
  /**
   * Returns the wrapped value as {@code BigDecimal}.
   *
   * @return {@code BigDecimal} value
   * @see BigDecimal
   */
  public BigDecimal getBigDecimal() {
    BigDecimal decimal;

    if (isBigDecimal()) {
      decimal = Bytes.toBigDecimal(rawBytes, OFFSET, rawBytes.length - OFFSET);
    } else if (isFloat()) {
      decimal = BigDecimal.valueOf(Bytes.toFloat(rawBytes, OFFSET));
    } else if (isDouble())  {
      decimal = BigDecimal.valueOf(Bytes.toDouble(rawBytes, OFFSET));
    } else if (isInt()) {
      decimal = BigDecimal.valueOf(Bytes.toInt(rawBytes, OFFSET));
    } else if (isLong()) {
      decimal = BigDecimal.valueOf(Bytes.toLong(rawBytes, OFFSET));
    } else if (isString()) {
      decimal = new BigDecimal(
        Bytes.toString(rawBytes, OFFSET, rawBytes.length - OFFSET));
    } else {
      throw new ClassCastException(
        "Cannot covert " + this.getType().getSimpleName() +
          " to " + Double.class.getSimpleName());
    }
    return decimal;
  }
  /**
   * Returns the wrapped value as {@code GradoopId}.
   *
   * @return {@code GradoopId} value
   */
  public GradoopId getGradoopId() {
    return GradoopId.fromByteArray(
      Arrays.copyOfRange(rawBytes, OFFSET, GradoopId.ID_SIZE + OFFSET));
  }

  /**
   * Returns the wrapped Map as {@code Map<PropertyValue, PropertyValue>}.
   *
   * @return {@code Map<PropertyValue, PropertyValue>} value
   */
  public Map<PropertyValue, PropertyValue> getMap() {
    PropertyValue key;
    PropertyValue value;

    Map<PropertyValue, PropertyValue> map = new HashMap<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

    try {
      if (inputStream.skipBytes(OFFSET) != OFFSET) {
        throw new RuntimeException("Malformed entry in PropertyValue List");
      }
      while (inputStream.available() > 0) {
        key = new PropertyValue();
        key.read(inputView);

        value = new PropertyValue();
        value.read(inputView);

        map.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return map;
  }

  /**
   * Returns the wrapped List as {@code List<PropertyValue>}.
   *
   * @return {@code List<PropertyValue>} value
   */
  public List<PropertyValue> getList() {
    PropertyValue entry;

    List<PropertyValue> list = new ArrayList<>();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
    DataInputStream inputStream = new DataInputStream(byteStream);
    DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

    try {
      if (inputStream.skipBytes(OFFSET) != OFFSET) {
        throw new RuntimeException("Malformed entry in PropertyValue List");
      }
      while (inputStream.available() > 0) {
        entry = new PropertyValue();
        entry.read(inputView);

        list.add(entry);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading PropertyValue");
    }

    return list;
  }
  /**
   * Returns the wrapped List as {@code LocalDate}.
   *
   * @return {@code LocalDate} value
   */
  public LocalDate getDate() {
    return DateTimeSerializer.deserializeDate(
      Arrays.copyOfRange(rawBytes, OFFSET, DateTimeSerializer.SIZEOF_DATE + OFFSET));
  }
  /**
   * Returns the wrapped List as {@code LocalTime}.
   *
   * @return {@code LocalTime} value
   */
  public LocalTime getTime() {
    return DateTimeSerializer.deserializeTime(
      Arrays.copyOfRange(rawBytes, OFFSET, DateTimeSerializer.SIZEOF_TIME + OFFSET));
  }
  /**
   * Returns the wrapped List as {@code LocalDateTime}.
   *
   * @return {@code LocalDateTime} value
   */
  public LocalDateTime getDateTime() {
    return DateTimeSerializer.deserializeDateTime(
      Arrays.copyOfRange(rawBytes, OFFSET, DateTimeSerializer.SIZEOF_DATETIME + OFFSET));
  }

  //----------------------------------------------------------------------------
  // Setter
  //----------------------------------------------------------------------------

  /**
   * Sets the given value as internal value if it has a supported type.
   *
   * @param value value
   * @throws UnsupportedTypeException
   */
  public void setObject(Object value) {
    if (value == null) {
      rawBytes = new byte[] {TYPE_NULL};
      validateBytesLength();
    } else if (value instanceof Boolean) {
      setBoolean((Boolean) value);
    } else if (value instanceof Integer) {
      setInt((Integer) value);
    } else if (value instanceof Long) {
      setLong((Long) value);
    } else if (value instanceof Float) {
      setFloat((Float) value);
    } else if (value instanceof Double) {
      setDouble((Double) value);
    } else if (value instanceof String) {
      setString((String) value);
    } else if (value instanceof BigDecimal) {
      setBigDecimal((BigDecimal) value);
    } else if (value instanceof GradoopId) {
      setGradoopId((GradoopId) value);
    } else if (value instanceof Map) {
      setMap((Map) value);
    } else if (value instanceof List) {
      setList((List) value);
    } else if (value instanceof LocalDate) {
      setDate((LocalDate) value);
    } else if (value instanceof LocalTime) {
      setTime((LocalTime) value);
    } else if (value instanceof LocalDateTime) {
      setDateTime((LocalDateTime) value);
    } else {
      throw new UnsupportedTypeException(value.getClass());
    }
  }
  /**
   * Sets the wrapped value as {@code boolean} value.
   *
   * @param booleanValue value
   */
  public void setBoolean(boolean booleanValue) {
    rawBytes = new byte[OFFSET + Bytes.SIZEOF_BOOLEAN];
    rawBytes[0] = TYPE_BOOLEAN;
    Bytes.putByte(rawBytes, OFFSET, (byte) (booleanValue ? -1 : 0));
  }
  /**
   * Sets the wrapped value as {@code int} value.
   *
   * @param intValue intValue
   */
  public void setInt(int intValue) {
    rawBytes = new byte[OFFSET + Bytes.SIZEOF_INT];
    rawBytes[0] = TYPE_INTEGER;
    Bytes.putInt(rawBytes, OFFSET, intValue);
  }
  /**
   * Sets the wrapped value as {@code long} value.
   *
   * @param longValue value
   */
  public void setLong(long longValue) {
    rawBytes = new byte[OFFSET + Bytes.SIZEOF_LONG];
    rawBytes[0] = TYPE_LONG;
    Bytes.putLong(rawBytes, OFFSET, longValue);
  }
  /**
   * Sets the wrapped value as {@code float} value.
   *
   * @param floatValue value
   */
  public void setFloat(float floatValue) {
    rawBytes = new byte[OFFSET + Bytes.SIZEOF_FLOAT];
    rawBytes[0] = TYPE_FLOAT;
    Bytes.putFloat(rawBytes, OFFSET, floatValue);
  }
  /**
   * Sets the wrapped value as {@code double} value.
   *
   * @param doubleValue value
   */
  public void setDouble(double doubleValue) {
    rawBytes = new byte[OFFSET + Bytes.SIZEOF_DOUBLE];
    rawBytes[0] = TYPE_DOUBLE;
    Bytes.putDouble(rawBytes, OFFSET, doubleValue);
  }
  /**
   * Sets the wrapped value as {@code String} value.
   *
   * @param stringValue value
   */
  public void setString(String stringValue) {
    byte[] valueBytes = Bytes.toBytes(stringValue);
    rawBytes = new byte[OFFSET + valueBytes.length];
    rawBytes[0] = TYPE_STRING;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
    validateBytesLength();
  }
  /**
   * Sets the wrapped value as {@code BigDecimal} value.
   *
   * @param bigDecimalValue value
   */
  public void setBigDecimal(BigDecimal bigDecimalValue) {
    byte[] valueBytes = Bytes.toBytes(bigDecimalValue);
    rawBytes = new byte[OFFSET + valueBytes.length];
    rawBytes[0] = TYPE_BIG_DECIMAL;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
    validateBytesLength();
  }
  /**
   * Sets the wrapped value as {@code GradoopId} value.
   *
   * @param gradoopIdValue value
   */
  public void setGradoopId(GradoopId gradoopIdValue) {
    byte[] valueBytes = gradoopIdValue.toByteArray();
    rawBytes = new byte[OFFSET + GradoopId.ID_SIZE];
    rawBytes[0] = TYPE_GRADOOP_ID;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
  }

  /**
   * Sets the wrapped value as {@code Map} value.
   *
   * @param map value
   */
  public void setMap(Map<PropertyValue, PropertyValue> map) {
    int size =
      map.keySet().stream().mapToInt(PropertyValue::byteSize).sum() +
      map.values().stream().mapToInt(PropertyValue::byteSize).sum() +
      OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(TYPE_MAP);
      for (Map.Entry<PropertyValue, PropertyValue> entry : map.entrySet()) {
        entry.getKey().write(outputView);
        entry.getValue().write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue");
    }

    this.rawBytes = byteStream.toByteArray();
    validateBytesLength();
  }

  /**
   * Sets the wrapped value as {@code List} value.
   *
   * @param list value
   */
  public void setList(List<PropertyValue> list) {
    int size = list.stream().mapToInt(PropertyValue::byteSize).sum() +
      OFFSET;

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
    DataOutputStream outputStream = new DataOutputStream(byteStream);
    DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

    try {
      outputStream.write(TYPE_LIST);
      for (PropertyValue entry : list) {
        entry.write(outputView);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing PropertyValue");
    }

    this.rawBytes = byteStream.toByteArray();
    validateBytesLength();
  }

  /**
   * Sets the wrapped value as {@code LocalDate} value.
   *
   * @param date value
   */
  public void setDate(LocalDate date) {
    byte[] valueBytes = DateTimeSerializer.serializeDate(date);
    rawBytes = new byte[OFFSET + DateTimeSerializer.SIZEOF_DATE];
    rawBytes[0] = TYPE_DATE;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
  }
  /**
   * Sets the wrapped value as {@code LocalTime} value.
   *
   * @param time value
   */
  public void setTime(LocalTime time) {
    byte[] valueBytes = DateTimeSerializer.serializeTime(time);
    rawBytes = new byte[OFFSET + DateTimeSerializer.SIZEOF_TIME];
    rawBytes[0] = TYPE_TIME;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
  }
  /**
   * Sets the wrapped value as {@code LocalDateTime} value.
   *
   * @param dateTime value
   */
  public void setDateTime(LocalDateTime dateTime) {
    byte[] valueBytes = DateTimeSerializer.serializeDateTime(dateTime);
    rawBytes = new byte[OFFSET + DateTimeSerializer.SIZEOF_DATETIME];
    rawBytes[0] = TYPE_DATETIME;
    Bytes.putBytes(rawBytes, OFFSET, valueBytes, 0, valueBytes.length);
  }

  //----------------------------------------------------------------------------
  // Util
  //----------------------------------------------------------------------------

  public Class<?> getType() {
    return rawBytes[0] == TYPE_BOOLEAN ?
      Boolean.class     : rawBytes[0] == TYPE_INTEGER     ?
      Integer.class     : rawBytes[0] == TYPE_LONG        ?
      Long.class        : rawBytes[0] == TYPE_FLOAT       ?
      Float.class       : rawBytes[0] == TYPE_DOUBLE      ?
      Double.class      : rawBytes[0] == TYPE_STRING      ?
      String.class      : rawBytes[0] == TYPE_BIG_DECIMAL ?
      BigDecimal.class  : rawBytes[0] == TYPE_GRADOOP_ID  ?
      GradoopId.class   : rawBytes[0] == TYPE_MAP         ?
      Map.class         : rawBytes[0] == TYPE_LIST        ?
      LocalDate.class   : rawBytes[0] == TYPE_DATE        ?
      LocalTime.class   : rawBytes[0] == TYPE_TIME        ?
      LocalDateTime.class : rawBytes[0] == TYPE_DATETIME  ?
      List.class        : null;
  }

  public int getByteSize() {
    return rawBytes.length;
  }

  @SuppressWarnings("EI_EXPOSE_REP")
  public byte[] getRawBytes() {
    return this.rawBytes;
  }

  /**
   * Set internal byte representation
   * @param bytes array
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  public void setBytes(byte[] bytes) {
    this.rawBytes = bytes;
    validateBytesLength();
  }

  /**
   * Create a {@link PropertyValue} that wraps a byte array
   * @param rawBytes array to wrap
   * @return new instance of {@link PropertyValue}
   */
  public static PropertyValue fromRawBytes(byte[] rawBytes) {
    return new PropertyValue(rawBytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PropertyValue)) {
      return false;
    }
    PropertyValue that = (PropertyValue) o;
    return Arrays.equals(rawBytes, that.rawBytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(rawBytes);
  }

  @Override
  public int compareTo(PropertyValue o) {
    int result;

    if (this.isNull() && o.isNull()) {
      result = 0;
    } else if (this.isBoolean() && o.isBoolean()) {
      result = Boolean.compare(this.getBoolean(), o.getBoolean());
    } else if (this.isInt() && o.isInt()) {
      result = Integer.compare(this.getInt(), o.getInt());
    } else if (this.isLong() && o.isLong()) {
      result = Long.compare(this.getLong(), o.getLong());
    } else if (this.isFloat() && o.isFloat()) {
      result = Float.compare(this.getFloat(), o.getFloat());
    } else if (this.isDouble() && o.isDouble()) {
      result = Double.compare(this.getDouble(), o.getDouble());
    } else if (this.isString() && o.isString()) {
      result = this.getString().compareTo(o.getString());
    } else if (this.isBigDecimal() && o.isBigDecimal()) {
      result = this.getBigDecimal().compareTo(o.getBigDecimal());
    } else if (this.isGradoopId() && o.isGradoopId()) {
      result = this.getGradoopId().compareTo(o.getGradoopId());
    } else if (this.isMap() || o.isMap() || this.isList() || o.isList()) {
      throw new UnsupportedOperationException(String.format(
        "Method compareTo() is not supported for %s, %s", this.getClass(), o.getClass()));
    } else {
      throw new IllegalArgumentException(String.format(
        "Incompatible types: %s, %s", this.getClass(), o.getClass()));
    }

    return result;
  }

  /**
   * Returns the byte size of the properties internal representation
   * @return byte size
   */
  public int byteSize() {
    return rawBytes.length;
  }

  /**
   * Byte representation:
   *
   * byte 1       : type info
   *
   * for dynamic length types (e.g. String and BigDecimal)
   * byte 2       : length (short)
   * byte 3       : length (short)
   * byte 4 - end : value bytes
   *
   * for fixed length types (e.g. int, long, float, ...)
   * byte 2 - end : value bytes
   *
   * @param outputView data output to write data to
   * @throws IOException
   */
  @Override
  public void write(DataOutputView outputView) throws IOException {
    // null?
    // type
    outputView.writeByte(rawBytes[0]);
    // dynamic type?
    if (rawBytes[0] == TYPE_STRING || rawBytes[0] == TYPE_BIG_DECIMAL ||
      rawBytes[0] == TYPE_MAP || rawBytes[0] == TYPE_LIST) {
      // write length
      outputView.writeShort(rawBytes.length - OFFSET);
    }
    // write data
    outputView.write(rawBytes, OFFSET, rawBytes.length - OFFSET);
  }

  @Override
  public void read(DataInputView inputView) throws IOException {
    short length = 0;
    // type
    byte type = inputView.readByte();
    // dynamic type?
    if (type == TYPE_STRING || type == TYPE_BIG_DECIMAL || type == TYPE_MAP || type == TYPE_LIST) {
      // read length
      length = inputView.readShort();
    } else if (type == TYPE_NULL) {
      length = 0;
    } else if (type == TYPE_BOOLEAN) {
      length = Bytes.SIZEOF_BOOLEAN;
    } else if (type == TYPE_INTEGER) {
      length = Bytes.SIZEOF_INT;
    } else if (type == TYPE_LONG) {
      length = Bytes.SIZEOF_LONG;
    } else if (type == TYPE_FLOAT) {
      length = Bytes.SIZEOF_FLOAT;
    } else if (type == TYPE_DOUBLE) {
      length = Bytes.SIZEOF_DOUBLE;
    } else if (type == TYPE_GRADOOP_ID) {
      length = GradoopId.ID_SIZE;
    } else if (type == TYPE_DATE) {
      length = DateTimeSerializer.SIZEOF_DATE;
    } else if (type == TYPE_TIME) {
      length = DateTimeSerializer.SIZEOF_TIME;
    } else if (type == TYPE_DATETIME) {
      length = DateTimeSerializer.SIZEOF_DATETIME;
    }
    // init new array
    rawBytes = new byte[OFFSET + length];
    // read type info
    rawBytes[0] = type;
    // read data
    for (int i = OFFSET; i < rawBytes.length; i++) {
      rawBytes[i] = inputView.readByte();
    }
  }

  @Override
  public String toString() {
    return getObject() != null ?
      getObject().toString() :
      GradoopConstants.NULL_STRING;
  }

  /**
   * Throw a runtime exception if this property value can't be represented
   * in {@link PropertyValue#MAX_BINARY_LENGTH} bytes.
   */
  private void validateBytesLength() {
    if (rawBytes != null && rawBytes.length > MAX_BINARY_LENGTH) {
      throw new IllegalStateException("The binary representation of this property is too big: " +
      rawBytes.length + " > " + MAX_BINARY_LENGTH);
    }
  }
}
