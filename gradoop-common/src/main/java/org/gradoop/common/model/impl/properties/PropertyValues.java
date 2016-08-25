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

import org.gradoop.common.storage.exceptions.UnsupportedTypeException;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities for operations on multiple property values.
 */
public class PropertyValues {

  /**
   * Boolean utilities.
   */
  public static class Boolean {

    /**
     * Logical or of two boolean properties.
     *
     * @param a first value
     * @param b second value
     * @return a OR b
     */
    public static PropertyValue or(PropertyValue a, PropertyValue b) {
      checkNotNull(a, b);
      checkBoolean(a);
      checkBoolean(b);

      a.setBoolean(a.getBoolean() || b.getBoolean());

      return a;
    }

    /**
     * Checks if a property value is boolean.
     *
     * @param value property value
     */
    private static void checkBoolean(PropertyValue value) {
      if (!value.isBoolean()) {
        throw new UnsupportedTypeException(value.getObject().getClass());
      }
    }
  }

  /**
   * Numeric utilities.
   */
  public static class Numeric {

    /**
     * Integer type.
     */
    private static final int INT = 1;
    /**
     * Long type.
     */
    private static final int LONG = 2;
    /**
     * Float type.
     */
    private static final int FLOAT = 3;
    /**
     * Double type.
     */
    private static final int DOUBLE = 4;
    /**
     * Big decimal type.
     */
    private static final int BIG_DECIMAL = 5;

    /**
     * Adds two numerical property values.
     *
     * @param aValue first value
     * @param bValue second value
     *
     * @return first value + second value
     */
    public static PropertyValue add(
      PropertyValue aValue, PropertyValue bValue) {

      int aType = checkNumericalAndGetType(aValue);
      int bType = checkNumericalAndGetType(bValue);

      boolean sameType = aType == bType;

      int returnType = sameType ? aType : Math.max(aType, bType);

      if (returnType == INT)  {
        aValue.setInt(aValue.getInt() + bValue.getInt());

      } else if (returnType == LONG)  {
        long a = aType == LONG ? aValue.getLong() : aValue.getInt();
        long b = bType == LONG ? bValue.getLong() : bValue.getInt();
        aValue.setLong(a + b);

      } else if (returnType == FLOAT)  {
        float a = aType == FLOAT ?
          aValue.getFloat() : floatValue(aValue, aType);
        float b = bType == FLOAT ?
          bValue.getFloat() : floatValue(bValue, bType);
        aValue.setFloat(a + b);

      } else if (returnType == DOUBLE)  {
        double a = aType == DOUBLE ?
          aValue.getDouble() : doubleValue(aValue, aType);
        double b = bType == DOUBLE ?
          bValue.getDouble() : doubleValue(bValue, bType);
        aValue.setDouble(a + b);

      } else {
        BigDecimal a = aType == BIG_DECIMAL ?
          aValue.getBigDecimal() : bigDecimalValue(aValue, aType);
        BigDecimal b = bType == BIG_DECIMAL ?
          bValue.getBigDecimal() : bigDecimalValue(bValue, bType);
        aValue.setBigDecimal(a.add(b));
      }

      return aValue;
    }

    /**
     * Multiplies two numerical property values.
     *
     * @param aValue first value
     * @param bValue second value
     *
     * @return first value * second value
     */
    public static PropertyValue multiply(
      PropertyValue aValue, PropertyValue bValue) {

      int aType = checkNumericalAndGetType(aValue);
      int bType = checkNumericalAndGetType(bValue);

      boolean sameType = aType == bType;

      int returnType = sameType ? aType : Math.max(aType, bType);

      if (returnType == INT)  {
        aValue.setInt(aValue.getInt() * bValue.getInt());

      } else if (returnType == LONG)  {
        long a = aType == LONG ? aValue.getLong() : aValue.getInt();
        long b = bType == LONG ? bValue.getLong() : bValue.getInt();
        aValue.setLong(a * b);

      } else if (returnType == FLOAT)  {
        float a = aType == FLOAT ?
          aValue.getFloat() : floatValue(aValue, aType);
        float b = bType == FLOAT ?
          bValue.getFloat() : floatValue(bValue, bType);
        aValue.setFloat(a * b);

      } else if (returnType == DOUBLE)  {
        double a = aType == DOUBLE ?
          aValue.getDouble() : doubleValue(aValue, aType);
        double b = bType == DOUBLE ?
          bValue.getDouble() : doubleValue(bValue, bType);
        aValue.setDouble(a * b);

      } else {
        BigDecimal a = aType == BIG_DECIMAL ?
          aValue.getBigDecimal() : bigDecimalValue(aValue, aType);
        BigDecimal b = bType == BIG_DECIMAL ?
          bValue.getBigDecimal() : bigDecimalValue(bValue, bType);
        aValue.setBigDecimal(a.multiply(b));
      }

      return aValue;
    }

    /**
     * Compares two numerical property values and returns the smaller one.
     *
     * @param a first value
     * @param b second value
     *
     * @return smaller value
     */
    public static PropertyValue min(PropertyValue a, PropertyValue b) {
      return isLessThan(a, b) ? a : b;
    }

    /**
     * Compares two numerical property values and returns the bigger one.
     *
     * @param a first value
     * @param b second value
     *
     * @return bigger value
     */
    public static PropertyValue max(PropertyValue a, PropertyValue b) {
      return isLessThan(a, b) ? b : a;
    }

    /**
     * Compares two numerical property values and returns true,
     * if the first one is smaller.
     *
     * @param a first value
     * @param b second value
     *
     * @return a < b
     */
    private static boolean isLessThan(PropertyValue a, PropertyValue b) {
      boolean aIsSmaller;
      int aType = checkNumericalAndGetType(a);
      int bType = checkNumericalAndGetType(b);

      if (aType == bType) {
        switch (aType) {
        case BIG_DECIMAL:
          aIsSmaller = a.getBigDecimal().compareTo(b.getBigDecimal()) <= 0;
          break;
        case DOUBLE:
          aIsSmaller = a.getDouble() <= b.getDouble();
          break;
        case FLOAT:
          aIsSmaller = a.getFloat() <= b.getFloat();
          break;
        case LONG:
          aIsSmaller = a.getLong() <= b.getLong();
          break;
        default:
          aIsSmaller = a.getInt() <= b.getInt();
          break;
        }
      } else if (aType > bType) {
        switch (aType) {
        case BIG_DECIMAL:
          aIsSmaller =
            a.getBigDecimal().compareTo(bigDecimalValue(b, bType)) <= 0;
          break;
        case DOUBLE:
          aIsSmaller = a.getDouble() <= doubleValue(b, bType);
          break;
        case FLOAT:
          aIsSmaller = a.getFloat() <= floatValue(b, bType);
          break;
        default:
          aIsSmaller = a.getLong() <= b.getInt();
          break;
        }
      } else {
        switch (bType) {
        case BIG_DECIMAL:
          aIsSmaller =
            b.getBigDecimal().compareTo(bigDecimalValue(a, aType)) <= 0;
          break;
        case DOUBLE:
          aIsSmaller = b.getDouble() > doubleValue(a, aType);
          break;
        case FLOAT:
          aIsSmaller = b.getFloat() > floatValue(a, aType);
          break;
        default:
          aIsSmaller = b.getLong() > a.getInt();
          break;
        }
      }
      return aIsSmaller;
    }

    /**
     * Checks a property value for numerical type and returns its type.
     *
     * @param value property value
     *
     * @return numerical type
     */
    private static int checkNumericalAndGetType(PropertyValue value) {
      checkNotNull(value);

      int type;

      if (value.isInt()) {
        type = INT;
      } else if (value.isLong()) {
        type = LONG;
      } else if (value.isFloat()) {
        type = FLOAT;
      } else if (value.isDouble()) {
        type = DOUBLE;
      } else if (value.isBigDecimal()) {
        type = BIG_DECIMAL;
      } else {
        throw new UnsupportedTypeException(value.getObject().getClass());
      }

      return type;
    }

    /**
     * Converts a value of a lower domain numerical type to BigDecimal.
     *
     * @param value value
     * @param type type
     *
     * @return converted value
     */
    private static BigDecimal bigDecimalValue(PropertyValue value, int type) {
      switch (type) {
      case INT:
        return BigDecimal.valueOf(value.getInt());
      case LONG:
        return BigDecimal.valueOf(value.getDouble());
      case FLOAT:
        return BigDecimal.valueOf(value.getFloat());
      default:
        return BigDecimal.valueOf(value.getDouble());
      }
    }

    /**
     * Converts a value of a lower domain numerical type to Double.
     *
     * @param value value
     * @param type type
     *
     * @return converted value
     */
    private static double doubleValue(PropertyValue value, int type) {
      switch (type) {
      case INT:
        return value.getInt();
      case LONG:
        return value.getLong();
      default:
        return value.getFloat();
      }
    }

    /**
     * Converts a value of a lower domain numerical type to Float.
     *
     * @param value value
     * @param type type
     *
     * @return converted value
     */
    private static float floatValue(PropertyValue value, int type) {
      switch (type) {
      case INT:
        return value.getInt();
      default:
        return value.getLong();
      }
    }
  }
}
