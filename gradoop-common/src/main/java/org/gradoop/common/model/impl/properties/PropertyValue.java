/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.util.GradoopConstants;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

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
   * {@code <property-type>} for {@link BigDecimal}
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
   * {@code <property-type>} for {@link LocalDate}
   */
  public static final transient byte TYPE_DATE         = 0x0b;
  /**
   * {@code <property-type>} for {@link LocalTime}
   */
  public static final transient byte TYPE_TIME         = 0x0c;
  /**
   * {@code <property-type>} for {@link LocalDateTime}
   */
  public static final transient byte TYPE_DATETIME     = 0x0d;
  /**
   * {@code <property-type>} for {@link java.lang.Short}
   */
  public static final transient byte TYPE_SHORT        = 0x0e;
  /**
   * {@code <property-type>} for {@link java.util.Set}
   */
  public static final transient byte TYPE_SET          = 0x0f;

  /**
   * Value offset in byte
   */
  public static final transient byte OFFSET            = 0x01;

  /**
   * Bit flag indicating a "large" property. The length of the byte representation will be stored
   * as an {@code int} instead.
   *
   * @see #write(DataOutputView)
   */
  public static final transient byte FLAG_LARGE = 0x10;

  /**
   * If the length of the byte representation is larger than this value, the length will be
   * stored as an {@code int} instead of a {@code short}.
   *
   * @see #write(DataOutputView)
   */
  public static final transient int LARGE_PROPERTY_THRESHOLD = Short.MAX_VALUE;

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Mapping from byte value to associated Class
   */
  private static final Map<Byte, Class> TYPE_MAPPING = LegacyPropertyValue.getTypeMap();
  private final LegacyPropertyValue legacyPropertyValue = new LegacyPropertyValue();
  private Object value;

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
    legacyPropertyValue.setObject(value);
  }

  /**
   * Creates a new property value from the given byte array.
   *
   * @param bytes byte array
   */
  private PropertyValue(byte[] bytes) {
    setBytes(bytes);
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

  /**
   * Create a {@link PropertyValue} that wraps a byte array
   *
   * @param rawBytes array to wrap
   * @return new instance of {@link PropertyValue}
   */
  public static PropertyValue fromRawBytes(byte[] rawBytes) {
    return new PropertyValue(rawBytes);
  }

  /**
   * Creates a deep copy of the property value.
   *
   * @return property value
   */
  public PropertyValue copy() {
    return legacyPropertyValue.copy();
  }

  //----------------------------------------------------------------------------
  // Type checking
  //----------------------------------------------------------------------------

  public boolean is(Class c) {
    return PropertyValueStrategy.PropertyValueStrategyFactory.get(c).is(value);
  }

  /**
   * True, if the value represents {@code null}.
   *
   * @return true, if {@code null} value
   */
  public boolean isNull() {
    return legacyPropertyValue.isNull();
  }

  /**
   * True, if the wrapped value is of type {@code boolean}.
   *
   * @return true, if {@code boolean} value
   */
  public boolean isBoolean() {
    return is(Boolean.class);
  }
  /**
   * True, if the wrapped value is of type {@code short}.
   *
   * @return true, if {@code short} value
   */
  public boolean isShort() {
    return legacyPropertyValue.isShort();
  }
  /**
   * True, if the wrapped value is of type {@code int}.
   *
   * @return true, if {@code int} value
   */
  public boolean isInt() {
    return legacyPropertyValue.isInt();
  }
  /**
   * True, if the wrapped value is of type {@code long}.
   *
   * @return true, if {@code long} value
   */
  public boolean isLong() {
    return legacyPropertyValue.isLong();
  }
  /**
   * True, if the wrapped value is of type {@code float}.
   *
   * @return true, if {@code float} value
   */
  public boolean isFloat() {
    return legacyPropertyValue.isFloat();
  }
  /**
   * True, if the wrapped value is of type {@code double}.
   *
   * @return true, if {@code double} value
   */
  public boolean isDouble() {
    return legacyPropertyValue.isDouble();
  }
  /**
   * True, if the wrapped value is of type {@code String}.
   *
   * @return true, if {@code String} value
   */
  public boolean isString() {
    return legacyPropertyValue.isString();
  }
  /**
   * True, if the wrapped value is of type {@code BigDecimal}.
   *
   * @return true, if {@code BigDecimal} value
   * @see BigDecimal
   */
  public boolean isBigDecimal() {
    return legacyPropertyValue.isBigDecimal();
  }
  /**
   * True, if the wrapped value is of type {@code GradoopId}.
   *
   * @return true, if {@code GradoopId} value
   */
  public boolean isGradoopId() {
    return legacyPropertyValue.isGradoopId();
  }
  /**
   * True, if the wrapped value is of type {@code Map}.
   *
   * @return true, if {@code Map} value
   */
  public boolean isMap() {
    return legacyPropertyValue.isMap();
  }
  /**
   * True, if the wrapped value is of type {@code List}.
   *
   * @return true, if {@code List} value
   */
  public boolean isList() {
    return legacyPropertyValue.isList();
  }
  /**
   * True, if the wrapped value is of type {@code LocalDate}.
   *
   * @return true, if {@code LocalDate} value
   */
  public boolean isDate() {
    return legacyPropertyValue.isDate();
  }
  /**
   * True, if the wrapped value is of type {@code LocalTime}.
   *
   * @return true, if {@code LocalTime} value
   */
  public boolean isTime() {
    return legacyPropertyValue.isTime();
  }
  /**
   * True, if the wrapped value is of type {@code LocalDateTime}.
   *
   * @return true, if {@code LocalDateTime} value
   */
  public boolean isDateTime() {
    return legacyPropertyValue.isDateTime();
  }
  /**
   * True, if the wrapped value is of type {@code Set}.
   *
   * @return true, if {@code Set} value
   */
  public boolean isSet() {
    return legacyPropertyValue.isSet();
  }

  /**
   * True, if the wrapped value is a subtype of {@code Number}.
   *
   * @return true, if {@code Number} value
   */
  public boolean isNumber() {
    return legacyPropertyValue.isNumber();
  }

  //----------------------------------------------------------------------------
  // Getter
  //----------------------------------------------------------------------------

  public <T> T get(Class<T> c) {
    PropertyValueStrategy strategy = PropertyValueStrategy.PropertyValueStrategyFactory.get(c);
    if (strategy.is(value)) {
      return (T) value;
    }
    return null;
  }

  /**
   * Returns the wrapped value as object.
   *
   * @return value or {@code null} if the value is empty
   */
  public Object getObject() {
    Object obj = null;
    if (value != null) {
      obj = get(value.getClass());
    }
    if (obj == null) {
      obj = legacyPropertyValue.getObject();
    }

    return obj;
  }
  /**
   * Returns the wrapped value as {@code boolean}.
   *
   * @return {@code boolean} value
   */
  public boolean getBoolean() {
    return get(Boolean.class);
  }

  /**
   * Returns the wrapped value as {@code short}.
   *
   * @return {@code short} value
   */
  public short getShort() {
    return legacyPropertyValue.getShort();
  }
  /**
   * Returns the wrapped value as {@code int}.
   *
   * @return {@code int} value
   */
  public int getInt() {
    return legacyPropertyValue.getInt();
  }
  /**
   * Returns the wrapped value as {@code long}.
   *
   * @return {@code long} value
   */
  public long getLong() {
    return legacyPropertyValue.getLong();
  }
  /**
   * Returns the wrapped value as {@code float}.
   *
   * @return {@code float} value
   */
  public float getFloat() {
    return legacyPropertyValue.getFloat();
  }
  /**
   * Returns the wrapped value as {@code double}.
   *
   * @return {@code double} value
   */
  public double getDouble() {
    return legacyPropertyValue.getDouble();
  }
  /**
   * Returns the wrapped value as {@code String}.
   *
   * @return {@code String} value
   */
  public String getString() {
    return legacyPropertyValue.getString();
  }
  /**
   * Returns the wrapped value as {@code BigDecimal}.
   *
   * @return {@code BigDecimal} value
   * @see BigDecimal
   */
  public BigDecimal getBigDecimal() {

    return legacyPropertyValue.getBigDecimal();
  }
  /**
   * Returns the wrapped value as {@code GradoopId}.
   *
   * @return {@code GradoopId} value
   */
  public GradoopId getGradoopId() {
    return legacyPropertyValue.getGradoopId();
  }

  /**
   * Returns the wrapped Map as {@code Map<PropertyValue, PropertyValue>}.
   *
   * @return {@code Map<PropertyValue, PropertyValue>} value
   */
  public Map<PropertyValue, PropertyValue> getMap() {

    return legacyPropertyValue.getMap();
  }

  /**
   * Returns the wrapped List as {@code List<PropertyValue>}.
   *
   * @return {@code List<PropertyValue>} value
   */
  public List<PropertyValue> getList() {

    return legacyPropertyValue.getList();
  }
  /**
   * Returns the wrapped List as {@code LocalDate}.
   *
   * @return {@code LocalDate} value
   */
  public LocalDate getDate() {
    return legacyPropertyValue.getDate();
  }
  /**
   * Returns the wrapped List as {@code LocalTime}.
   *
   * @return {@code LocalTime} value
   */
  public LocalTime getTime() {
    return legacyPropertyValue.getTime();
  }
  /**
   * Returns the wrapped List as {@code LocalDateTime}.
   *
   * @return {@code LocalDateTime} value
   */
  public LocalDateTime getDateTime() {
    return legacyPropertyValue.getDateTime();
  }
  /**
   * Returns the wrapped Set as {@code Set<PropertyValue>}.
   *
   * @return {@code Set<PropertyValue>} value
   */
  public Set<PropertyValue> getSet() {

    return legacyPropertyValue.getSet();
  }

  //----------------------------------------------------------------------------
  // Setter
  //----------------------------------------------------------------------------

  /**
   * Sets the given value as internal value if it has a supported type.
   *
   * @param value value
   * @throws UnsupportedTypeException if the type of the Object is not supported
   */
  public void setObject(Object value) {
    this.value = value;
    if (value == null || !is(value.getClass())) {
      legacyPropertyValue.setObject(value);
    }
  }
  /**
   * Sets the wrapped value as {@code boolean} value.
   *
   * @param booleanValue value
   */
  public void setBoolean(boolean booleanValue) {
    setObject(booleanValue);
  }

  /**
   * Sets the wrapped value as {@code short} value.
   *
   * @param shortValue value
   */
  public void setShort(short shortValue) {
    legacyPropertyValue.setShort(shortValue);
  }
  /**
   * Sets the wrapped value as {@code int} value.
   *
   * @param intValue intValue
   */
  public void setInt(int intValue) {
    legacyPropertyValue.setInt(intValue);
  }
  /**
   * Sets the wrapped value as {@code long} value.
   *
   * @param longValue value
   */
  public void setLong(long longValue) {
    legacyPropertyValue.setLong(longValue);
  }
  /**
   * Sets the wrapped value as {@code float} value.
   *
   * @param floatValue value
   */
  public void setFloat(float floatValue) {
    legacyPropertyValue.setFloat(floatValue);
  }
  /**
   * Sets the wrapped value as {@code double} value.
   *
   * @param doubleValue value
   */
  public void setDouble(double doubleValue) {
    legacyPropertyValue.setDouble(doubleValue);
  }
  /**
   * Sets the wrapped value as {@code String} value.
   *
   * @param stringValue value
   */
  public void setString(String stringValue) {
    legacyPropertyValue.setString(stringValue);
  }
  /**
   * Sets the wrapped value as {@code BigDecimal} value.
   *
   * @param bigDecimalValue value
   */
  public void setBigDecimal(BigDecimal bigDecimalValue) {
    legacyPropertyValue.setBigDecimal(bigDecimalValue);
  }
  /**
   * Sets the wrapped value as {@code GradoopId} value.
   *
   * @param gradoopIdValue value
   */
  public void setGradoopId(GradoopId gradoopIdValue) {
    legacyPropertyValue.setGradoopId(gradoopIdValue);
  }

  /**
   * Sets the wrapped value as {@code Map} value.
   *
   * @param map value
   */
  public void setMap(Map<PropertyValue, PropertyValue> map) {

    legacyPropertyValue.setMap(map);
  }

  /**
   * Sets the wrapped value as {@code List} value.
   *
   * @param list value
   */
  public void setList(List<PropertyValue> list) {

    legacyPropertyValue.setList(list);
  }

  /**
   * Sets the wrapped value as {@code LocalDate} value.
   *
   * @param date value
   */
  public void setDate(LocalDate date) {
    legacyPropertyValue.setDate(date);
  }
  /**
   * Sets the wrapped value as {@code LocalTime} value.
   *
   * @param time value
   */
  public void setTime(LocalTime time) {
    legacyPropertyValue.setTime(time);
  }
  /**
   * Sets the wrapped value as {@code LocalDateTime} value.
   *
   * @param dateTime value
   */
  public void setDateTime(LocalDateTime dateTime) {
    legacyPropertyValue.setDateTime(dateTime);
  }

  /**
   * Sets the wrapped value as {@code Set} value.
   *
   * @param set value
   */
  public void setSet(Set<PropertyValue> set) {

    legacyPropertyValue.setSet(set);
  }

  //----------------------------------------------------------------------------
  // Util
  //----------------------------------------------------------------------------

  /**
   * Get the data type as class object according to the first position of the rawBytes[] array
   *
   * @return Class object
   */
  public Class<?> getType() {
    Class<?> c = null;
    if (value != null) {
      c = PropertyValueStrategy.PropertyValueStrategyFactory.get(value.getClass()).getType();
    }

    if (c == null) {
      c = legacyPropertyValue.getType();
    }

    return c;
  }

  public int getByteSize() {
    return getRawBytes().length;
  }

  @SuppressWarnings("EI_EXPOSE_REP")
  public byte[] getRawBytes() {
    byte[] rawBytes = null;
    if (value != null) {
      rawBytes = PropertyValueStrategy.PropertyValueStrategyFactory.get(value.getClass()).getRawBytes(value);
    }
    if (rawBytes == null) {
      rawBytes = legacyPropertyValue.getRawBytes();
    }
    return rawBytes;
  }

  /**
   * Set internal byte representation
   * @param bytes array
   */
  @SuppressWarnings("EI_EXPOSE_REP")
  public void setBytes(byte[] bytes) {
    value = PropertyValueStrategy.PropertyValueStrategyFactory.fromRawBytes(bytes);
    legacyPropertyValue.setBytes(bytes);
  }

  @Override
  public boolean equals(Object o) {
    return legacyPropertyValue.equals(o) || (o instanceof PropertyValue && Objects.equals(value, ((PropertyValue)o).value));
  }

  @Override
  public int hashCode() {
    byte[] rawBytes = null;
    if (value != null) {
      rawBytes = PropertyValueStrategy.PropertyValueStrategyFactory.get(value.getClass()).getRawBytes(value);
    }
    return rawBytes == null ? legacyPropertyValue.hashCode() : Arrays.hashCode(rawBytes);
  }

  @Override
  public int compareTo(PropertyValue o) {

    return legacyPropertyValue.compareTo(o);
  }

  /**
   * Returns the byte size of the properties internal representation
   * @return byte size
   */
  public int byteSize() {
    return legacyPropertyValue.byteSize();
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
   * If the size of the internal byte representation if larger than
   * {@link #LARGE_PROPERTY_THRESHOLD} (i.e. if a {@code short} is too small to store the length),
   * then the {@link #FLAG_LARGE} bit will be set in the first byte and the byte representation
   * will be:
   * byte 2       ; length (int)
   * byte 3       : length (int)
   * byte 4       : length (int)
   * byte 5       : length (int)
   * byte 6 - end : value bytes
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
    // Write type.
    // Write length for types with a variable length.
    // write data
    legacyPropertyValue.write(outputView);
  }

  @Override
  public void read(DataInputView inputView) throws IOException {
    // type
    // Apply bitmask to get the actual type.
    // dynamic type?
    // init new array
    // read type info
    // read data
    legacyPropertyValue.read(inputView);
  }

  @Override
  public String toString() {
    return legacyPropertyValue.toString();
  }

  public static class LegacyPropertyValue implements Comparable<PropertyValue>, Serializable, Value {
    /**
     * Stores the type and the value
     */
    private byte[] rawBytes;

    public LegacyPropertyValue() {
    }

    /**
     * Creates a deep copy of the property value.
     *
     * @return property value
     */
    private PropertyValue copy() {
      return PropertyValue.create(getObject());
    }

    /**
     * True, if the value represents {@code null}.
     *
     * @return true, if {@code null} value
     */
    private boolean isNull() {
      return rawBytes[0] == PropertyValue.TYPE_NULL;
    }

    /**
     * True, if the wrapped value is of type {@code boolean}.
     *
     * @return true, if {@code boolean} value
     */
    private boolean isBoolean() {
      return rawBytes[0] == PropertyValue.TYPE_BOOLEAN;
    }

    /**
     * True, if the wrapped value is of type {@code short}.
     *
     * @return true, if {@code short} value
     */
    private boolean isShort() {
      return rawBytes[0] == PropertyValue.TYPE_SHORT;
    }

    /**
     * True, if the wrapped value is of type {@code int}.
     *
     * @return true, if {@code int} value
     */
    private boolean isInt() {
      return rawBytes[0] == PropertyValue.TYPE_INTEGER;
    }

    /**
     * True, if the wrapped value is of type {@code long}.
     *
     * @return true, if {@code long} value
     */
    private boolean isLong() {
      return rawBytes[0] == PropertyValue.TYPE_LONG;
    }

    /**
     * True, if the wrapped value is of type {@code float}.
     *
     * @return true, if {@code float} value
     */
    private boolean isFloat() {
      return rawBytes[0] == PropertyValue.TYPE_FLOAT;
    }

    /**
     * True, if the wrapped value is of type {@code double}.
     *
     * @return true, if {@code double} value
     */
    private boolean isDouble() {
      return rawBytes[0] == PropertyValue.TYPE_DOUBLE;
    }

    /**
     * True, if the wrapped value is of type {@code String}.
     *
     * @return true, if {@code String} value
     */
    private boolean isString() {
      return rawBytes[0] == PropertyValue.TYPE_STRING;
    }

    /**
     * True, if the wrapped value is of type {@code BigDecimal}.
     *
     * @return true, if {@code BigDecimal} value
     * @see BigDecimal
     */
    private boolean isBigDecimal() {
      return rawBytes[0] == PropertyValue.TYPE_BIG_DECIMAL;
    }

    /**
     * True, if the wrapped value is of type {@code GradoopId}.
     *
     * @return true, if {@code GradoopId} value
     */
    private boolean isGradoopId() {
      return rawBytes[0] == PropertyValue.TYPE_GRADOOP_ID;
    }

    /**
     * True, if the wrapped value is of type {@code Map}.
     *
     * @return true, if {@code Map} value
     */
    private boolean isMap() {
      return rawBytes[0] == PropertyValue.TYPE_MAP;
    }

    /**
     * True, if the wrapped value is of type {@code List}.
     *
     * @return true, if {@code List} value
     */
    private boolean isList() {
      return rawBytes[0] == PropertyValue.TYPE_LIST;
    }

    /**
     * True, if the wrapped value is of type {@code LocalDate}.
     *
     * @return true, if {@code LocalDate} value
     */
    private boolean isDate() {
      return rawBytes[0] == PropertyValue.TYPE_DATE;
    }

    /**
     * True, if the wrapped value is of type {@code LocalTime}.
     *
     * @return true, if {@code LocalTime} value
     */
    private boolean isTime() {
      return rawBytes[0] == PropertyValue.TYPE_TIME;
    }

    /**
     * True, if the wrapped value is of type {@code LocalDateTime}.
     *
     * @return true, if {@code LocalDateTime} value
     */
    private boolean isDateTime() {
      return rawBytes[0] == PropertyValue.TYPE_DATETIME;
    }

    /**
     * True, if the wrapped value is of type {@code Set}.
     *
     * @return true, if {@code Set} value
     */
    private boolean isSet() {
      return rawBytes[0] == PropertyValue.TYPE_SET;
    }

    /**
     * True, if the wrapped value is a subtype of {@code Number}.
     *
     * @return true, if {@code Number} value
     */
    private boolean isNumber() {
      return !isNull() && Number.class.isAssignableFrom(getType());
    }

    /**
     * Returns the wrapped value as object.
     *
     * @return value or {@code null} if the value is empty
     */
    private Object getObject() {
      return isBoolean() ? getBoolean() :
              isShort() ? getShort() :
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
                                                                                                                      isSet() ? getSet() :
                                                                                                                              null;
    }

    /**
     * Returns the wrapped value as {@code boolean}.
     *
     * @return {@code boolean} value
     */
    private boolean getBoolean() {
      return rawBytes[1] == -1;
    }

    /**
     * Returns the wrapped value as {@code short}.
     *
     * @return {@code short} value
     */
    private short getShort() {
      return Bytes.toShort(rawBytes, PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code int}.
     *
     * @return {@code int} value
     */
    private int getInt() {
      return Bytes.toInt(rawBytes, PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code long}.
     *
     * @return {@code long} value
     */
    private long getLong() {
      return Bytes.toLong(rawBytes, PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code float}.
     *
     * @return {@code float} value
     */
    private float getFloat() {
      return Bytes.toFloat(rawBytes, PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code double}.
     *
     * @return {@code double} value
     */
    private double getDouble() {
      return Bytes.toDouble(rawBytes, PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code String}.
     *
     * @return {@code String} value
     */
    private String getString() {
      return Bytes.toString(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
    }

    /**
     * Returns the wrapped value as {@code BigDecimal}.
     *
     * @return {@code BigDecimal} value
     * @see BigDecimal
     */
    private BigDecimal getBigDecimal() {
      BigDecimal decimal;

      if (isBigDecimal()) {
        decimal = Bytes.toBigDecimal(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
      } else if (isFloat()) {
        decimal = BigDecimal.valueOf(getFloat());
      } else if (isDouble()) {
        decimal = BigDecimal.valueOf(getDouble());
      } else if (isShort()) {
        decimal = BigDecimal.valueOf(getShort());
      } else if (isInt()) {
        decimal = BigDecimal.valueOf(getInt());
      } else if (isLong()) {
        decimal = BigDecimal.valueOf(getLong());
      } else if (isString()) {
        decimal = new BigDecimal(getString());
      } else {
        throw new ClassCastException(
                "Cannot covert " + this.getType().getSimpleName() +
                        " to " + BigDecimal.class.getSimpleName());
      }
      return decimal;
    }

    /**
     * Returns the wrapped value as {@code GradoopId}.
     *
     * @return {@code GradoopId} value
     */
    private GradoopId getGradoopId() {
      return GradoopId.fromByteArray(
              Arrays.copyOfRange(rawBytes, PropertyValue.OFFSET, GradoopId.ID_SIZE + PropertyValue.OFFSET));
    }

    /**
     * Returns the wrapped Map as {@code Map<PropertyValue, PropertyValue>}.
     *
     * @return {@code Map<PropertyValue, PropertyValue>} value
     */
    private Map<PropertyValue, PropertyValue> getMap() {
      PropertyValue key;
      PropertyValue value;

      Map<PropertyValue, PropertyValue> map = new HashMap<PropertyValue, PropertyValue>();

      ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
      DataInputStream inputStream = new DataInputStream(byteStream);
      DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

      try {
        if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
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
    private List<PropertyValue> getList() {
      PropertyValue entry;

      List<PropertyValue> list = new ArrayList<PropertyValue>();

      ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
      DataInputStream inputStream = new DataInputStream(byteStream);
      DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

      try {
        if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
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
    private LocalDate getDate() {
      return DateTimeSerializer.deserializeDate(
              Arrays.copyOfRange(rawBytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATE + PropertyValue.OFFSET));
    }

    /**
     * Returns the wrapped List as {@code LocalTime}.
     *
     * @return {@code LocalTime} value
     */
    private LocalTime getTime() {
      return DateTimeSerializer.deserializeTime(
              Arrays.copyOfRange(rawBytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_TIME + PropertyValue.OFFSET));
    }

    /**
     * Returns the wrapped List as {@code LocalDateTime}.
     *
     * @return {@code LocalDateTime} value
     */
    private LocalDateTime getDateTime() {
      return DateTimeSerializer.deserializeDateTime(
              Arrays.copyOfRange(rawBytes, PropertyValue.OFFSET, DateTimeSerializer.SIZEOF_DATETIME + PropertyValue.OFFSET));
    }

    /**
     * Returns the wrapped Set as {@code Set<PropertyValue>}.
     *
     * @return {@code Set<PropertyValue>} value
     */
    private Set<PropertyValue> getSet() {
      PropertyValue entry;

      Set<PropertyValue> set = new HashSet<PropertyValue>();

      ByteArrayInputStream byteStream = new ByteArrayInputStream(rawBytes);
      DataInputStream inputStream = new DataInputStream(byteStream);
      DataInputView inputView = new DataInputViewStreamWrapper(inputStream);

      try {
        if (inputStream.skipBytes(PropertyValue.OFFSET) != PropertyValue.OFFSET) {
          throw new RuntimeException("Malformed entry in PropertyValue Set");
        }
        while (inputStream.available() > 0) {
          entry = new PropertyValue();
          entry.read(inputView);

          set.add(entry);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading PropertyValue");
      }

      return set;
    }

    /**
     * Sets the given value as internal value if it has a supported type.
     *
     * @param value value
     * @throws UnsupportedTypeException if the type of the Object is not supported
     */
    private void setObject(Object value) {
      if (value == null) {
        rawBytes = new byte[]{PropertyValue.TYPE_NULL};
      } else if (value instanceof Boolean) {
        setBoolean((Boolean) value);
      } else if (value instanceof Short) {
        setShort((Short) value);
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
      } else if (value instanceof Set) {
        setSet((Set) value);
      } else {
        throw new UnsupportedTypeException(value.getClass());
      }
    }

    /**
     * Sets the wrapped value as {@code boolean} value.
     *
     * @param booleanValue value
     */
    private void setBoolean(boolean booleanValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_BOOLEAN];
      rawBytes[0] = PropertyValue.TYPE_BOOLEAN;
      Bytes.putByte(rawBytes, PropertyValue.OFFSET, (byte) (booleanValue ? -1 : 0));
    }

    /**
     * Sets the wrapped value as {@code short} value.
     *
     * @param shortValue value
     */
    private void setShort(short shortValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_SHORT];
      rawBytes[0] = PropertyValue.TYPE_SHORT;
      Bytes.putShort(rawBytes, PropertyValue.OFFSET, shortValue);
    }

    /**
     * Sets the wrapped value as {@code int} value.
     *
     * @param intValue intValue
     */
    private void setInt(int intValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_INT];
      rawBytes[0] = PropertyValue.TYPE_INTEGER;
      Bytes.putInt(rawBytes, PropertyValue.OFFSET, intValue);
    }

    /**
     * Sets the wrapped value as {@code long} value.
     *
     * @param longValue value
     */
    private void setLong(long longValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_LONG];
      rawBytes[0] = PropertyValue.TYPE_LONG;
      Bytes.putLong(rawBytes, PropertyValue.OFFSET, longValue);
    }

    /**
     * Sets the wrapped value as {@code float} value.
     *
     * @param floatValue value
     */
    private void setFloat(float floatValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_FLOAT];
      rawBytes[0] = PropertyValue.TYPE_FLOAT;
      Bytes.putFloat(rawBytes, PropertyValue.OFFSET, floatValue);
    }

    /**
     * Sets the wrapped value as {@code double} value.
     *
     * @param doubleValue value
     */
    private void setDouble(double doubleValue) {
      rawBytes = new byte[PropertyValue.OFFSET + Bytes.SIZEOF_DOUBLE];
      rawBytes[0] = PropertyValue.TYPE_DOUBLE;
      Bytes.putDouble(rawBytes, PropertyValue.OFFSET, doubleValue);
    }

    /**
     * Sets the wrapped value as {@code String} value.
     *
     * @param stringValue value
     */
    private void setString(String stringValue) {
      byte[] valueBytes = Bytes.toBytes(stringValue);
      rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
      rawBytes[0] = PropertyValue.TYPE_STRING;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code BigDecimal} value.
     *
     * @param bigDecimalValue value
     */
    private void setBigDecimal(BigDecimal bigDecimalValue) {
      byte[] valueBytes = Bytes.toBytes(bigDecimalValue);
      rawBytes = new byte[PropertyValue.OFFSET + valueBytes.length];
      rawBytes[0] = PropertyValue.TYPE_BIG_DECIMAL;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code GradoopId} value.
     *
     * @param gradoopIdValue value
     */
    private void setGradoopId(GradoopId gradoopIdValue) {
      byte[] valueBytes = gradoopIdValue.toByteArray();
      rawBytes = new byte[PropertyValue.OFFSET + GradoopId.ID_SIZE];
      rawBytes[0] = PropertyValue.TYPE_GRADOOP_ID;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code Map} value.
     *
     * @param map value
     */
    private void setMap(Map<PropertyValue, PropertyValue> map) {
      int size =
              map.keySet().stream().mapToInt(PropertyValue::byteSize).sum() +
                      map.values().stream().mapToInt(PropertyValue::byteSize).sum() +
                      PropertyValue.OFFSET;

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

      try {
        outputStream.write(PropertyValue.TYPE_MAP);
        for (Map.Entry<PropertyValue, PropertyValue> entry : map.entrySet()) {
          entry.getKey().write(outputView);
          entry.getValue().write(outputView);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error writing PropertyValue");
      }

      this.rawBytes = byteStream.toByteArray();
    }

    /**
     * Sets the wrapped value as {@code List} value.
     *
     * @param list value
     */
    private void setList(List<PropertyValue> list) {
      int size = list.stream().mapToInt(PropertyValue::byteSize).sum() +
              PropertyValue.OFFSET;

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

      try {
        outputStream.write(PropertyValue.TYPE_LIST);
        for (PropertyValue entry : list) {
          entry.write(outputView);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error writing PropertyValue");
      }

      this.rawBytes = byteStream.toByteArray();
    }

    /**
     * Sets the wrapped value as {@code LocalDate} value.
     *
     * @param date value
     */
    private void setDate(LocalDate date) {
      byte[] valueBytes = DateTimeSerializer.serializeDate(date);
      rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATE];
      rawBytes[0] = PropertyValue.TYPE_DATE;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code LocalTime} value.
     *
     * @param time value
     */
    private void setTime(LocalTime time) {
      byte[] valueBytes = DateTimeSerializer.serializeTime(time);
      rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_TIME];
      rawBytes[0] = PropertyValue.TYPE_TIME;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code LocalDateTime} value.
     *
     * @param dateTime value
     */
    private void setDateTime(LocalDateTime dateTime) {
      byte[] valueBytes = DateTimeSerializer.serializeDateTime(dateTime);
      rawBytes = new byte[PropertyValue.OFFSET + DateTimeSerializer.SIZEOF_DATETIME];
      rawBytes[0] = PropertyValue.TYPE_DATETIME;
      Bytes.putBytes(rawBytes, PropertyValue.OFFSET, valueBytes, 0, valueBytes.length);
    }

    /**
     * Sets the wrapped value as {@code Set} value.
     *
     * @param set value
     */
    private void setSet(Set<PropertyValue> set) {
      int size = set.stream().mapToInt(PropertyValue::byteSize).sum() + PropertyValue.OFFSET;

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

      try {
        outputStream.write(PropertyValue.TYPE_SET);
        for (PropertyValue entry : set) {
          entry.write(outputView);
        }
      } catch (IOException e) {
        throw new RuntimeException("Error writing PropertyValue");
      }

      this.rawBytes = byteStream.toByteArray();
    }

    /**
     * Get the data type as class object according to the first position of the rawBytes[] array
     *
     * @return Class object
     */
    private Class<?> getType() {
      return PropertyValue.TYPE_MAPPING.get(rawBytes[0]);
    }

    /**
     * Creates a type mapping HashMap to assign a byte value to its represented Class
     *
     * @return a Map with byte to class assignments
     */
    private static Map<Byte, Class> getTypeMap() {
      Map<Byte, Class> map = new HashMap<Byte, Class>();
      map.put(PropertyValue.TYPE_BOOLEAN, Boolean.class);
      map.put(PropertyValue.TYPE_SHORT, Short.class);
      map.put(PropertyValue.TYPE_INTEGER, Integer.class);
      map.put(PropertyValue.TYPE_LONG, Long.class);
      map.put(PropertyValue.TYPE_FLOAT, Float.class);
      map.put(PropertyValue.TYPE_DOUBLE, Double.class);
      map.put(PropertyValue.TYPE_STRING, String.class);
      map.put(PropertyValue.TYPE_BIG_DECIMAL, BigDecimal.class);
      map.put(PropertyValue.TYPE_GRADOOP_ID, GradoopId.class);
      map.put(PropertyValue.TYPE_MAP, Map.class);
      map.put(PropertyValue.TYPE_LIST, List.class);
      map.put(PropertyValue.TYPE_DATE, LocalDate.class);
      map.put(PropertyValue.TYPE_TIME, LocalTime.class);
      map.put(PropertyValue.TYPE_DATETIME, LocalDateTime.class);
      map.put(PropertyValue.TYPE_SET, Set.class);
      return Collections.unmodifiableMap(map);
    }

    private int getByteSize() {
      return rawBytes.length;
    }

    @SuppressWarnings("EI_EXPOSE_REP")
    private byte[] getRawBytes() {
      return this.rawBytes;
    }

    /**
     * Set internal byte representation
     *
     * @param bytes array
     */
    @SuppressWarnings("EI_EXPOSE_REP")
    private void setBytes(byte[] bytes) {
      this.rawBytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (null == o) {
        return true;
      }
      if (!(o instanceof PropertyValue)) {
        return false;
      }
      PropertyValue that = (PropertyValue) o;
      return Arrays.equals(rawBytes, that.getRawBytes());
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
      } else if (this.isNumber() && o.isNumber()) {
        result = PropertyValueUtils.Numeric.compare(PropertyValue.fromRawBytes(rawBytes), o);
      } else if (this.isBoolean() && o.isBoolean()) {
        result = Boolean.compare(this.getBoolean(), o.getBoolean());
      } else if (this.isString() && o.isString()) {
        result = this.getString().compareTo(o.getString());
      } else if (this.isGradoopId() && o.isGradoopId()) {
        result = this.getGradoopId().compareTo(o.getGradoopId());
      } else if (this.isDate() && o.isDate()) {
        result = this.getDate().compareTo(o.getDate());
      } else if (this.isTime() && o.isTime()) {
        result = this.getTime().compareTo(o.getTime());
      } else if (this.isDateTime() && o.isDateTime()) {
        result = this.getDateTime().compareTo(o.getDateTime());
      } else if (this.isMap() || o.isMap() ||
              this.isList() || o.isList() ||
              this.isSet() || o.isSet()) {
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
     *
     * @return byte size
     */
    private int byteSize() {
      return rawBytes.length;
    }

    /**
     * Byte representation:
     * <p>
     * byte 1       : type info
     * <p>
     * for dynamic length types (e.g. String and BigDecimal)
     * byte 2       : length (short)
     * byte 3       : length (short)
     * byte 4 - end : value bytes
     * <p>
     * If the size of the internal byte representation if larger than
     * {@link #LARGE_PROPERTY_THRESHOLD} (i.e. if a {@code short} is too small to store the length),
     * then the {@link #FLAG_LARGE} bit will be set in the first byte and the byte representation
     * will be:
     * byte 2       ; length (int)
     * byte 3       : length (int)
     * byte 4       : length (int)
     * byte 5       : length (int)
     * byte 6 - end : value bytes
     * <p>
     * for fixed length types (e.g. int, long, float, ...)
     * byte 2 - end : value bytes
     *
     * @param outputView data output to write data to
     * @throws IOException
     */
    @Override
    public void write(DataOutputView outputView) throws IOException {
      // null?
      // Write type.
      byte type = rawBytes[0];
      if (rawBytes.length > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
        type |= PropertyValue.FLAG_LARGE;
      }
      outputView.writeByte(type);
      // Write length for types with a variable length.
      if (isString() || isBigDecimal() || isMap() || isList() || isSet()) {
        // Write length as an int if the "large" flag is set.
        if ((type & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
          outputView.writeInt(rawBytes.length - PropertyValue.OFFSET);
        } else {
          outputView.writeShort(rawBytes.length - PropertyValue.OFFSET);
        }
      }
      // write data
      outputView.write(rawBytes, PropertyValue.OFFSET, rawBytes.length - PropertyValue.OFFSET);
    }

    @Override
    public void read(DataInputView inputView) throws IOException {
      int length = 0;
      // type
      byte typeByte = inputView.readByte();
      // Apply bitmask to get the actual type.
      byte type = (byte) (~PropertyValue.FLAG_LARGE & typeByte);
      // dynamic type?
      if (type == PropertyValue.TYPE_STRING || type == PropertyValue.TYPE_BIG_DECIMAL || type == PropertyValue.TYPE_MAP ||
              type == PropertyValue.TYPE_LIST || type == PropertyValue.TYPE_SET) {
        // read length
        if ((typeByte & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
          length = inputView.readInt();
        } else {
          length = inputView.readShort();
        }
      } else if (type == PropertyValue.TYPE_NULL) {
        length = 0;
      } else if (type == PropertyValue.TYPE_BOOLEAN) {
        length = Bytes.SIZEOF_BOOLEAN;
      } else if (type == PropertyValue.TYPE_SHORT) {
        length = Bytes.SIZEOF_SHORT;
      } else if (type == PropertyValue.TYPE_INTEGER) {
        length = Bytes.SIZEOF_INT;
      } else if (type == PropertyValue.TYPE_LONG) {
        length = Bytes.SIZEOF_LONG;
      } else if (type == PropertyValue.TYPE_FLOAT) {
        length = Bytes.SIZEOF_FLOAT;
      } else if (type == PropertyValue.TYPE_DOUBLE) {
        length = Bytes.SIZEOF_DOUBLE;
      } else if (type == PropertyValue.TYPE_GRADOOP_ID) {
        length = GradoopId.ID_SIZE;
      } else if (type == PropertyValue.TYPE_DATE) {
        length = DateTimeSerializer.SIZEOF_DATE;
      } else if (type == PropertyValue.TYPE_TIME) {
        length = DateTimeSerializer.SIZEOF_TIME;
      } else if (type == PropertyValue.TYPE_DATETIME) {
        length = DateTimeSerializer.SIZEOF_DATETIME;
      }
      // init new array
      rawBytes = new byte[PropertyValue.OFFSET + length];
      // read type info
      rawBytes[0] = type;
      // read data
      for (int i = PropertyValue.OFFSET; i < rawBytes.length; i++) {
        rawBytes[i] = inputView.readByte();
      }
    }

    @Override
    public String toString() {
      return getObject() != null ?
              getObject().toString() :
              GradoopConstants.NULL_STRING;
    }
  }

}
