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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.strategies.PropertyValueStrategyFactory;
import org.gradoop.common.util.GradoopConstants;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
   * Stores the object representation of the value
   */
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
   * Create a {@link PropertyValue} that wraps a byte array.
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
    return new PropertyValue(getRawBytes());
  }

  //----------------------------------------------------------------------------
  // Type checking
  //----------------------------------------------------------------------------

  /**
   * Check if the property value type is an instance of a certain class.
   *
   * @param clazz class to check against
   * @return true if the attribute {@code value} is an object of the provided class, false
   * otherwise
   */
  public boolean is(Class clazz) {
    return PropertyValueStrategyFactory.get(clazz).is(value);
  }

  /**
   * True, if the value represents {@code null}.
   *
   * @return true, if {@code null} value
   */
  public boolean isNull() {
    return getRawBytes()[0] == PropertyValue.TYPE_NULL;
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
    return is(Short.class);
  }

  /**
   * True, if the wrapped value is of type {@code int}.
   *
   * @return true, if {@code int} value
   */
  public boolean isInt() {
    return is(Integer.class);
  }

  /**
   * True, if the wrapped value is of type {@code long}.
   *
   * @return true, if {@code long} value
   */
  public boolean isLong() {
    return is(Long.class);
  }

  /**
   * True, if the wrapped value is of type {@code float}.
   *
   * @return true, if {@code float} value
   */
  public boolean isFloat() {
    return is(Float.class);
  }

  /**
   * True, if the wrapped value is of type {@code double}.
   *
   * @return true, if {@code double} value
   */
  public boolean isDouble() {
    return is(Double.class);
  }

  /**
   * True, if the wrapped value is of type {@link String}.
   *
   * @return true, if {@link String} value
   */
  public boolean isString() {
    return is(String.class);
  }

  /**
   * True, if the wrapped value is of type {@link BigDecimal}.
   *
   * @return true, if {@link BigDecimal} value
   * @see BigDecimal
   */
  public boolean isBigDecimal() {
    return is(BigDecimal.class);
  }

  /**
   * True, if the wrapped value is of type {@link GradoopId}.
   *
   * @return true, if {@link GradoopId} value
   */
  public boolean isGradoopId() {
    return is(GradoopId.class);
  }

  /**
   * True, if the wrapped value is of type {@link Map}.
   *
   * @return true, if {@link Map} value
   */
  public boolean isMap() {
    return is(Map.class);
  }

  /**
   * True, if the wrapped value is of type {@link List}.
   *
   * @return true, if {@link List} value
   */
  public boolean isList() {
    return is(List.class);
  }

  /**
   * True, if the wrapped value is of type {@link LocalDate}.
   *
   * @return true, if {@link LocalDate} value
   */
  public boolean isDate() {
    return is(LocalDate.class);
  }

  /**
   * True, if the wrapped value is of type {@link LocalTime}.
   *
   * @return true, if {@link LocalTime} value
   */
  public boolean isTime() {
    return is(LocalTime.class);
  }

  /**
   * True, if the wrapped value is of type {@link LocalDateTime}.
   *
   * @return true, if {@link LocalDateTime} value
   */
  public boolean isDateTime() {
    return is(LocalDateTime.class);
  }

  /**
   * True, if the wrapped value is of type {@link Set}.
   *
   * @return true, if {@link Set} value
   */
  public boolean isSet() {
    return is(Set.class);
  }

  /**
   * True, if the wrapped value is a subtype of {@link Number}.
   *
   * @return true, if {@link Number} value
   */
  public boolean isNumber() {
    return !isNull() && Number.class.isAssignableFrom(getType());
  }

  //----------------------------------------------------------------------------
  // Getter
  //----------------------------------------------------------------------------

  /**
   * Returns the value as the specified type. If it is already of the same type as requested, we
   * just return the value. If not, then we try to cast the value via the requested types strategy
   * get method.
   *
   * @param clazz the requested value type
   * @param <T> PropertyValue supported type
   * @return value
   */
  public <T> T get(Class<T> clazz) {
    PropertyValueStrategy strategy = PropertyValueStrategyFactory.get(clazz);
    if (strategy.is(value)) {
      return (T) value;
    }
    byte[] bytes = PropertyValueStrategyFactory.getRawBytes(value);
    try {
      return  (T) strategy.get(bytes);
    } catch (IOException e) {
      throw new RuntimeException("Cannot convert " + value + " to " + clazz);
    }
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
    return get(Short.class);
  }

  /**
   * Returns the wrapped value as {@code int}.
   *
   * @return {@code int} value
   */
  public int getInt() {
    return get(Integer.class);
  }

  /**
   * Returns the wrapped value as {@code long}.
   *
   * @return {@code long} value
   */
  public long getLong() {
    return get(Long.class);
  }

  /**
   * Returns the wrapped value as {@code float}.
   *
   * @return {@code float} value
   */
  public float getFloat() {
    return get(Float.class);
  }

  /**
   * Returns the wrapped value as {@code double}.
   *
   * @return {@code double} value
   */
  public double getDouble() {
    return get(Double.class);
  }

  /**
   * Returns the wrapped value as {@link String}.
   *
   * @return {@link String} value
   */
  public String getString() {
    return get(String.class);
  }

  /**
   * Returns the wrapped value as {@link BigDecimal}.
   *
   * @return {@link BigDecimal} value
   * @see BigDecimal
   */
  public BigDecimal getBigDecimal() {
    return get(BigDecimal.class);
  }

  /**
   * Returns the wrapped value as {@link GradoopId}.
   *
   * @return {@link GradoopId} value
   */
  public GradoopId getGradoopId() {
    return get(GradoopId.class);
  }

  /**
   * Returns the wrapped Map as {@code Map<PropertyValue, PropertyValue>}.
   *
   * @return {@code Map<PropertyValue, PropertyValue>} value
   */
  public Map<PropertyValue, PropertyValue> getMap() {
    return get(Map.class);
  }

  /**
   * Returns the wrapped List as {@code List<PropertyValue>}.
   *
   * @return {@code List<PropertyValue>} value
   */
  public List<PropertyValue> getList() {
    return get(List.class);
  }

  /**
   * Returns the wrapped List as {@link LocalDate}.
   *
   * @return {@link LocalDate} value
   */
  public LocalDate getDate() {
    return get(LocalDate.class);
  }

  /**
   * Returns the wrapped List as {@link LocalTime}.
   *
   * @return {@link LocalTime} value
   */
  public LocalTime getTime() {
    return get(LocalTime.class);
  }

  /**
   * Returns the wrapped List as {@link LocalDateTime}.
   *
   * @return {@link LocalDateTime} value
   */
  public LocalDateTime getDateTime() {
    return get(LocalDateTime.class);
  }

  /**
   * Returns the wrapped Set as {@code Set<PropertyValue>}.
   *
   * @return {@code Set<PropertyValue>} value
   */
  public Set<PropertyValue> getSet() {
    return get(Set.class);
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
    if (value != null && !PropertyValueStrategyFactory.get(value.getClass()).is(value)) {
      throw new UnsupportedTypeException(value.getClass());
    }
    this.value = value;
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
    setObject(shortValue);
  }

  /**
   * Sets the wrapped value as {@code int} value.
   *
   * @param intValue intValue
   */
  public void setInt(int intValue) {
    setObject(intValue);
  }

  /**
   * Sets the wrapped value as {@code long} value.
   *
   * @param longValue value
   */
  public void setLong(long longValue) {
    setObject(longValue);
  }

  /**
   * Sets the wrapped value as {@code float} value.
   *
   * @param floatValue value
   */
  public void setFloat(float floatValue) {
    setObject(floatValue);
  }

  /**
   * Sets the wrapped value as {@code double} value.
   *
   * @param doubleValue value
   */
  public void setDouble(double doubleValue) {
    setObject(doubleValue);
  }

  /**
   * Sets the wrapped value as {@link String} value.
   *
   * @param stringValue value
   */
  public void setString(String stringValue) {
    setObject(stringValue);
  }

  /**
   * Sets the wrapped value as {@link BigDecimal} value.
   *
   * @param bigDecimalValue value
   */
  public void setBigDecimal(BigDecimal bigDecimalValue) {
    setObject(bigDecimalValue);
  }

  /**
   * Sets the wrapped value as {@link GradoopId} value.
   *
   * @param gradoopIdValue value
   */
  public void setGradoopId(GradoopId gradoopIdValue) {
    setObject(gradoopIdValue);
  }

  /**
   * Sets the wrapped value as {@link Map} value.
   *
   * @param map value
   */
  public void setMap(Map<PropertyValue, PropertyValue> map) {
    setObject(map);
  }

  /**
   * Sets the wrapped value as {@link List} value.
   *
   * @param list value
   */
  public void setList(List<PropertyValue> list) {
    setObject(list);
  }

  /**
   * Sets the wrapped value as {@link LocalDate} value.
   *
   * @param date value
   */
  public void setDate(LocalDate date) {
    setObject(date);
  }

  /**
   * Sets the wrapped value as {@link LocalTime} value.
   *
   * @param time value
   */
  public void setTime(LocalTime time) {
    setObject(time);
  }

  /**
   * Sets the wrapped value as {@code LocalDateTime} value.
   *
   * @param dateTime value
   */
  public void setDateTime(LocalDateTime dateTime) {
    setObject(dateTime);
  }

  /**
   * Sets the wrapped value as {@code Set} value.
   *
   * @param set value
   */
  public void setSet(Set<PropertyValue> set) {
    setObject(set);
  }

  //----------------------------------------------------------------------------
  // Util
  //----------------------------------------------------------------------------

  /**
   * Get the data type as class object according to the first position of the rawBytes[] array.
   *
   * @return Class object
   */
  public Class<?> getType() {
    Class<?> clazz = null;
    if (value != null) {
      clazz = PropertyValueStrategyFactory.get(value.getClass()).getType();
    }

    return clazz;
  }

  public int getByteSize() {
    return getRawBytes().length;
  }

  public byte[] getRawBytes() {
    return PropertyValueStrategyFactory.getRawBytes(value);
  }

  /**
   * Set internal byte representation.
   *
   * @param bytes array
   */
  public void setBytes(byte[] bytes) {
    value = PropertyValueStrategyFactory.fromRawBytes(bytes);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof PropertyValue && Objects.equals(value, ((PropertyValue) object).value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(PropertyValueStrategyFactory.getRawBytes(value));
  }

  @Override
  public int compareTo(PropertyValue other) {
    return PropertyValueStrategyFactory.compare(value, other.value);
  }

  /**
   * Returns the byte size of the properties internal representation.
   *
   * @return byte size
   */
  public int byteSize() {
    byte[] rawBytes = PropertyValueStrategyFactory.getRawBytes(value);
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
   * @throws IOException if write to output view fails.
   */
  @Override
  public void write(DataOutputView outputView) throws IOException {
    PropertyValueStrategyFactory.get(value).write(value, outputView);
  }

  @Override
  public void read(DataInputView inputView) throws IOException {
    // type
    byte typeByte = inputView.readByte();
    // Apply bitmask to get the actual type.
    byte type = (byte) (~PropertyValue.FLAG_LARGE & typeByte);

    PropertyValueStrategy strategy = PropertyValueStrategyFactory.get(type);

    if (strategy == null) {
      throw new UnsupportedTypeException("No strategy for type byte from input view found");
    } else {
      value = strategy.read(inputView, typeByte);
    }
  }

  @Override
  public String toString() {
    return getObject() != null ?
      getObject().toString() :
      GradoopConstants.NULL_STRING;
  }
}
