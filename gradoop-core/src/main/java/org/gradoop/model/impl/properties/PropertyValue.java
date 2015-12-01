/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.properties;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.backend.hadoop.BigDecimalWritable;
import org.apache.pig.backend.hadoop.DateTimeWritable;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a single property value in the EPGM.
 *
 * A property value wraps a value that implements a supported data type.
 */
public class PropertyValue
  extends GenericWritable
  implements WritableComparable<PropertyValue> {

  /**
   * Supported classes. Must implement {@link WritableComparable}.
   *
   * Note that the order is necessary to correctly deserialize objects.
   */
  private static Class<? extends WritableComparable>[] TYPES;

  static {
    //noinspection unchecked
    TYPES = (Class<? extends WritableComparable>[]) new Class[] {
        BooleanWritable.class,
        IntWritable.class,
        LongWritable.class,
        FloatWritable.class,
        DoubleWritable.class,
        Text.class,
        BigDecimalWritable.class,
        DateTimeWritable.class
    };
  }

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
  public PropertyValue(Object value) {
    setObject(value);
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
   * True, if the wrapped value is of type {@code boolean}.
   *
   * @return true, if {@code boolean} value
   */
  public boolean isBoolean() {
    return get() instanceof BooleanWritable;
  }
  /**
   * True, if the wrapped value is of type {@code int}.
   *
   * @return true, if {@code int} value
   */
  public boolean isInt() {
    return get() instanceof IntWritable;
  }
  /**
   * True, if the wrapped value is of type {@code long}.
   *
   * @return true, if {@code long} value
   */
  public boolean isLong() {
    return get() instanceof LongWritable;
  }
  /**
   * True, if the wrapped value is of type {@code float}.
   *
   * @return true, if {@code float} value
   */
  public boolean isFloat() {
    return get() instanceof FloatWritable;
  }
  /**
   * True, if the wrapped value is of type {@code double}.
   *
   * @return true, if {@code double} value
   */
  public boolean isDouble() {
    return get() instanceof DoubleWritable;
  }
  /**
   * True, if the wrapped value is of type {@code String}.
   *
   * @return true, if {@code String} value
   */
  public boolean isString() {
    return get() instanceof Text;
  }
  /**
   * True, if the wrapped value is of type {@code BigDecimal}.
   *
   * @return true, if {@code BigDecimal} value
   * @see BigDecimal
   */
  public boolean isBigDecimal() {
    return get() instanceof BigDecimalWritable;
  }
  /**
   * True, if the wrapped value is of type {@code DateTime}.
   *
   * @return true, if {@code DateTime} value
   * @see DateTime
   */
  public boolean isDateTime() {
    return get() instanceof DateTimeWritable;
  }

  //----------------------------------------------------------------------------
  // Getter
  //----------------------------------------------------------------------------

  /**
   * Returns the wrapped value as object.
   *
   * @return value
   */
  public Object getObject() {
    return isBoolean() ? getBoolean() :
      isInt() ? getInt() :
        isLong() ? getLong() :
          isFloat() ? getFloat() :
            isDouble() ? getDouble() :
              isString() ? getString() :
                isBigDecimal() ? getBigDecimal() :
                  getDateTime();
  }
  /**
   * Returns the wrapped value as {@code boolean}.
   *
   * @return {@code boolean} value
   */
  public boolean getBoolean() {
    return ((BooleanWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code int}.
   *
   * @return {@code int} value
   */
  public int getInt() {
    return ((IntWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code long}.
   *
   * @return {@code long} value
   */
  public long getLong() {
    return ((LongWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code float}.
   *
   * @return {@code float} value
   */
  public float getFloat() {
    return ((FloatWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code double}.
   *
   * @return {@code double} value
   */
  public double getDouble() {
    return ((DoubleWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code String}.
   *
   * @return {@code String} value
   */
  public String getString() {
    return get().toString();
  }
  /**
   * Returns the wrapped value as {@code BigDecimal}.
   *
   * @return {@code BigDecimal} value
   * @see BigDecimal
   */
  public BigDecimal getBigDecimal() {
    return ((BigDecimalWritable) get()).get();
  }
  /**
   * Returns the wrapped value as {@code DateTime}.
   *
   * @return {@code DateTime} value
   * @see DateTime
   */
  public DateTime getDateTime() {
    return ((DateTimeWritable) get()).get();
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
    checkNotNull(value, "Property value was null");
    if (value instanceof Boolean) {
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
    } else if (value instanceof DateTime) {
      setDateTime((DateTime) value);
    } else {
      throw new UnsupportedTypeException(value.getClass());
    }
  }
  /**
   * Sets the wrapped value as {@code boolean} value.
   *
   * @param value value
   */
  public void setBoolean(boolean value) {
    set(new BooleanWritable(value));
  }
  /**
   * Sets the wrapped value as {@code int} value.
   *
   * @param value value
   */
  public void setInt(int value) {
    set(new IntWritable(value));
  }
  /**
   * Sets the wrapped value as {@code long} value.
   *
   * @param value value
   */
  public void setLong(long value) {
    set(new LongWritable(value));
  }
  /**
   * Sets the wrapped value as {@code float} value.
   *
   * @param value value
   */
  public void setFloat(float value) {
    set(new FloatWritable(value));
  }
  /**
   * Sets the wrapped value as {@code double} value.
   *
   * @param value value
   */
  public void setDouble(double value) {
    set(new DoubleWritable(value));
  }
  /**
   * Sets the wrapped value as {@code String} value.
   *
   * @param value value
   */
  public void setString(String value) {
    set(new Text(value));
  }
  /**
   * Sets the wrapped value as {@code BigDecimal} value.
   *
   * @param value value
   */
  public void setBigDecimal(BigDecimal value) {
    set(new BigDecimalWritable(value));
  }
  /**
   * Sets the wrapped value as {@code DateTime} value.
   *
   * @param value value
   */
  public void setDateTime(DateTime value) {
    set(new DateTimeWritable(value));
  }

  //----------------------------------------------------------------------------
  // Util
  //----------------------------------------------------------------------------

  @Override
  public int hashCode() {
    return get().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PropertyValue that = (PropertyValue) o;

    return this.get().equals(that.get());
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(PropertyValue o) {
    return ((WritableComparable) this.get()).compareTo(o.get());
  }

  @Override
  public String toString() {
    return isString() ? String.format("\"%s\"", getObject().toString()) :
      getObject().toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Class<? extends WritableComparable>[] getTypes() {
    return TYPES;
  }
}
