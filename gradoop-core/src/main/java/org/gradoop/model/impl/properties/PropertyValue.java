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
import org.gradoop.model.api.EPGMPropertyValue;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A property value wraps a {@link WritableComparable} for the specific type.
 * Comparison, equality, and de/serialization are delegated to the Writable.
 */
public class PropertyValue
  extends GenericWritable
  implements EPGMPropertyValue {

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
   * Supported classes. Must implement {@link WritableComparable}.
   *
   * Note that the order is necessary to correctly deserialize objects.
   */
  private static Class[] CLASSES = {
    BooleanWritable.class,
    IntWritable.class,
    LongWritable.class,
    FloatWritable.class,
    DoubleWritable.class,
    Text.class,
    BigDecimalWritable.class,
    DateTimeWritable.class
  };

  /**
   * Default constructor.
   */
  public PropertyValue() {}

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

  @Override
  public void setObject(Object value) {
    checkNotNull(value, "Property value was null");
    if (value instanceof Boolean) {
      setBoolean((Boolean) value);
    } else if (value instanceof Integer) {
      setInt((Integer) value);
    } else if (value instanceof Long) {
      setLong((Long) value);
    } else if (value instanceof Float) {
      setFloat((Float)value);
    } else if (value instanceof Double) {
      setDouble((Double)value);
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

  @Override
  public boolean isBoolean() {
    return get() instanceof BooleanWritable;
  }

  @Override
  public boolean getBoolean() {
    return ((BooleanWritable) get()).get();
  }

  @Override
  public void setBoolean(Boolean value) {
    set(new BooleanWritable(value));
  }

  @Override
  public boolean isInt() {
    return get() instanceof IntWritable;
  }

  @Override
  public int getInt() {
    return ((IntWritable) get()).get();
  }

  @Override
  public void setInt(Integer value) {
    set(new IntWritable(value));
  }

  @Override
  public boolean isLong() {
    return get() instanceof LongWritable;
  }

  @Override
  public long getLong() {
    return ((LongWritable) get()).get();
  }

  @Override
  public void setLong(Long value) {
    set(new LongWritable(value));
  }

  @Override
  public boolean isFloat() {
    return get() instanceof FloatWritable;
  }

  @Override
  public float getFloat() {
    return ((FloatWritable) get()).get();
  }

  @Override
  public void setFloat(Float value) {
    set(new FloatWritable(value));
  }

  @Override
  public boolean isDouble() {
    return get() instanceof DoubleWritable;
  }

  @Override
  public double getDouble() {
    return ((DoubleWritable) get()).get();
  }

  @Override
  public void setDouble(Double value) {
    set(new DoubleWritable(value));
  }

  @Override
  public boolean isString() {
    return get() instanceof Text;
  }

  @Override
  public String getString() {
    return get().toString();
  }

  @Override
  public void setString(String value) {
    set(new Text(value));
  }

  @Override
  public boolean isBigDecimal() {
    return get() instanceof BigDecimalWritable;
  }

  @Override
  public BigDecimal getBigDecimal() {
    return ((BigDecimalWritable) get()).get();
  }

  @Override
  public void setBigDecimal(BigDecimal value) {
    set(new BigDecimalWritable(value));
  }

  @Override
  public boolean isDateTime() {
    return get() instanceof DateTimeWritable;
  }

  @Override
  public DateTime getDateTime() {
    return ((DateTimeWritable) get()).get();
  }

  @Override
  public void setDateTime(DateTime value) {
    set(new DateTimeWritable(value));
  }

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
  public int compareTo(EPGMPropertyValue o) {
    int res;
    if (o instanceof PropertyValue) {
      // use the comparison methods of the Writable implementations
      res = ((WritableComparable) this.get())
        .compareTo(((PropertyValue) o).get());
    } else {
      res = (isBoolean() && o.isBoolean()) ?
        Boolean.compare(getBoolean(), o.getBoolean())
        : (isInt() && o.isInt()) ?
        Integer.compare(getInt(), o.getInt())
        : (isLong() && o.isLong()) ?
        Long.compare(getLong(), o.getLong())
        : (isFloat() && o.isFloat()) ?
        Float.compare(getFloat(), o.getFloat())
        : (isDouble() && o.isDouble()) ?
        Double.compare(getDouble(), o.getDouble())
        : (isString() && o.isString()) ?
        getString().compareTo(o.getString())
        : (isBigDecimal() && o.isBigDecimal()) ?
        getBigDecimal().compareTo(o.getBigDecimal())
        : (isDateTime() && o.isDateTime()) ?
        getDateTime().compareTo(o.getDateTime()) : 2;

      if (res == 2) {
        throw new IllegalArgumentException(String.format(
          "Incompatible wrapped types %s and %s", getClass(), o.getClass()));
      }
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Class<? extends WritableComparable>[] getTypes() {
    return CLASSES;
  }
}
