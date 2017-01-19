/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.model.impl.properties;

import com.google.common.primitives.Ints;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.gradoop.common.util.GConstants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
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
public class PropertyValue
  implements WritableComparable<PropertyValue>, Serializable {

  /**
   * Represents a property value that is {@code null}.
   */
  public static final PropertyValue NULL_VALUE = PropertyValue.create(null);

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * {@code <property-type>} for empty property value (i.e. {@code null})
   */
  private static final transient byte TYPE_NULL         = 0x00;
  /**
   * {@code <property-type>} for {@link java.lang.Boolean}
   */
  private static final transient byte TYPE_BOOLEAN      = 0x01;
  /**
   * {@code <property-type>} for {@link java.lang.Integer}
   */
  private static final transient byte TYPE_INTEGER      = 0x02;
  /**
   * {@code <property-type>} for {@link java.lang.Long}
   */
  private static final transient byte TYPE_LONG         = 0x03;
  /**
   * {@code <property-type>} for {@link java.lang.Float}
   */
  private static final transient byte TYPE_FLOAT        = 0x04;
  /**
   * {@code <property-type>} for {@link java.lang.Double}
   */
  private static final transient byte TYPE_DOUBLE       = 0x05;
  /**
   * {@code <property-type>} for {@link java.lang.String}
   */
  private static final transient byte TYPE_STRING       = 0x06;
  /**
   * {@code <property-type>} for {@link java.lang.String}
   */
  private static final transient byte TYPE_BIG_DECIMAL  = 0x07;
  /**
   * {@code <property-type>} for {@link org.gradoop.common.model.impl.id.GradoopId}
   */
  private static final transient byte TYPE_GRADOOP_ID   = 0x08;
  /**
   * {@code <property-type>} for {@link java.util.HashMap}
   */
  private static final transient byte TYPE_MAP          = 0x09;
  /**
   * {@code <property-type>} for {@link java.util.List}
   */
  private static final transient byte TYPE_LIST         = 0x0a;

  /**
   * Value offset in byte
   */
  private static final transient byte OFFSET            = 0x01;

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
    int offset = 1;
    int size;
    PropertyValue key;
    PropertyValue value;

    Map<PropertyValue, PropertyValue> map = new HashMap<>();

    while (offset < rawBytes.length) {
      size = Ints.fromByteArray(ArrayUtils.subarray(rawBytes, offset, offset + Integer.BYTES));
      offset += Integer.BYTES;

      key = new PropertyValue(ArrayUtils.subarray(rawBytes, offset, offset + size));
      offset += size;

      size = Ints.fromByteArray(ArrayUtils.subarray(rawBytes, offset, offset + Integer.BYTES));
      offset += Integer.BYTES;

      value = new PropertyValue(ArrayUtils.subarray(rawBytes, offset, offset + size));
      offset += size;

      map.put(key, value);
    }

    return map;
  }

  /**
   * Returns the wrapped List as {@code List<PropertyValue>}.
   *
   * @return {@code List<PropertyValue>} value
   */
  public List<PropertyValue> getList() {
    int offset = 1;
    int size;
    PropertyValue entry;

    List<PropertyValue> list = new ArrayList<>();

    while (offset < rawBytes.length) {
      size = Ints.fromByteArray(ArrayUtils.subarray(rawBytes, offset, offset + Integer.BYTES));
      offset += Integer.BYTES;

      entry = new PropertyValue(ArrayUtils.subarray(rawBytes, offset, offset + size));
      offset += size;

      list.add(entry);
    }

    return list;
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
      setMap((HashMap) value);
    } else if (value instanceof List) {
      setList((List) value);
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
      map.keySet().size() * Integer.BYTES +
      map.values().stream().mapToInt(PropertyValue::byteSize).sum() +
      map.values().size() * Integer.BYTES +
      1;

    rawBytes = new byte[size];
    rawBytes[0] = TYPE_MAP;

    int offset = 1;

    for (Map.Entry<PropertyValue, PropertyValue> entry : map.entrySet()) {
      System.arraycopy(
        Ints.toByteArray(entry.getKey().byteSize()), 0,
        rawBytes, offset,
        Integer.BYTES
      );
      offset += Integer.BYTES;

      System.arraycopy(
        entry.getKey().rawBytes, 0,
        rawBytes, offset,
        entry.getKey().byteSize()
      );
      offset += entry.getKey().byteSize();

      System.arraycopy(
        Ints.toByteArray(entry.getValue().byteSize()), 0,
        rawBytes, offset,
        Integer.BYTES
      );
      offset += Integer.BYTES;

      System.arraycopy(
        entry.getValue().rawBytes, 0,
        rawBytes, offset,
        entry.getValue().byteSize()
      );
      offset += entry.getValue().byteSize();
    }
  }

  /**
   * Sets the wrapped value as {@code List} value.
   *
   * @param list value
   */
  public void setList(List<PropertyValue> list) {
    int size = list.stream().mapToInt(PropertyValue::byteSize).sum() +
      list.size() * Integer.BYTES +
      1;

    rawBytes = new byte[size];
    rawBytes[0] = TYPE_LIST;

    int offset = 1;

    for (PropertyValue entry : list) {
      System.arraycopy(
        Ints.toByteArray(entry.byteSize()), 0,
        rawBytes, offset,
        Integer.BYTES
      );
      offset += Integer.BYTES;

      System.arraycopy(
        entry.rawBytes, 0,
        rawBytes, offset,
        entry.byteSize()
      );
      offset += entry.byteSize();
    }
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
      List.class        : null;
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
   * @param dataOutput data output to write data to
   * @throws IOException
   */
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    // null?
    // type
    dataOutput.writeByte(rawBytes[0]);
    // dynamic type?
    if (rawBytes[0] == TYPE_STRING || rawBytes[0] == TYPE_BIG_DECIMAL ||
      rawBytes[0] == TYPE_MAP || rawBytes[0] == TYPE_LIST) {
      // write length
      dataOutput.writeShort(rawBytes.length - OFFSET);
    }
    // write data
    dataOutput.write(rawBytes, OFFSET, rawBytes.length - OFFSET);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    short length = 0;
    // type
    byte type = dataInput.readByte();
    // dynamic type?
    if (type == TYPE_STRING || type == TYPE_BIG_DECIMAL ||
      type == TYPE_MAP || type == TYPE_LIST) {
      // read length
      length = dataInput.readShort();
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
    }
    // init new array
    rawBytes = new byte[OFFSET + length];
    // read type info
    rawBytes[0] = type;
    // read data
    for (int i = OFFSET; i < rawBytes.length; i++) {
      rawBytes[i] = dataInput.readByte();
    }
  }

  @Override
  public String toString() {
    return getObject() != null ?
      getObject().toString() :
      GConstants.NULL_STRING;
  }
}
