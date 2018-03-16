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

import org.gradoop.common.storage.exceptions.UnsupportedTypeException;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utilities for operations on multiple property values.
 */
public class PropertyValueUtils {

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
     * Float type.
     */
    private static final int FLOAT = 1;
    /**
     * Integer type.
     */
    private static final int INT = 2;
    /**
     * Double type.
     */
    private static final int DOUBLE = 3;
    /**
     * Long type.
     */
    private static final int LONG = 4;
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

      int returnType = sameType ? aType : maxType(aType, bType);

      if (returnType == INT)  {
        aValue.setInt(aValue.getInt() + bValue.getInt());

      } else if (returnType == FLOAT)  {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : aValue.getInt();
          b = bType == FLOAT ? bValue.getFloat() : bValue.getInt();
        }

        aValue.setFloat(a + b);

      } else if (returnType == LONG)  {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : aValue.getInt();
          b = bType == LONG ? bValue.getLong() : bValue.getInt();
        }

        aValue.setLong(a + b);

      } else if (returnType == DOUBLE)  {

        double a;
        double b;

        if (sameType) {
          a = aValue.getDouble();
          b = bValue.getDouble();
        } else {
          a = aType == DOUBLE ? aValue.getDouble() : doubleValue(aValue, aType);
          b = bType == DOUBLE ? bValue.getDouble() : doubleValue(bValue, bType);
        }

        aValue.setDouble(a + b);

      } else {

        BigDecimal a;
        BigDecimal b;

        if (sameType) {
          a = aValue.getBigDecimal();
          b = bValue.getBigDecimal();
        } else {
          a = aType == BIG_DECIMAL ?
            aValue.getBigDecimal() : bigDecimalValue(aValue, aType);
          b = bType == BIG_DECIMAL ?
            bValue.getBigDecimal() : bigDecimalValue(bValue, bType);
        }

        aValue.setBigDecimal(a.add(b));
      }

      return aValue;
    }

    /**
     * returns the maximum of two types
     *
     * @param aType first type
     * @param bType second type
     *
     * @return larger compatible type
     */
    private static int maxType(int aType, int bType) {

      int maxType = Math.max(aType, bType);

      if (maxType != BIG_DECIMAL && // not big decimal
        aType % 2 != bType % 2 && // different supertype
        maxType % 2 == 0) { // high supertype is integer

        // next decimal type
        maxType++;
      }

      return maxType;
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

      int returnType = sameType ? aType : maxType(aType, bType);

      if (returnType == INT)  {
        aValue.setInt(aValue.getInt() * bValue.getInt());

      } else if (returnType == FLOAT)  {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : aValue.getInt();
          b = bType == FLOAT ? bValue.getFloat() : bValue.getInt();
        }

        aValue.setFloat(a * b);

      } else if (returnType == LONG)  {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : aValue.getInt();
          b = bType == LONG ? bValue.getLong() : bValue.getInt();
        }

        aValue.setLong(a * b);

      } else if (returnType == DOUBLE)  {

        double a;
        double b;

        if (sameType) {
          a = aValue.getDouble();
          b = bValue.getDouble();
        } else {
          a = aType == DOUBLE ? aValue.getDouble() : doubleValue(aValue, aType);
          b = bType == DOUBLE ? bValue.getDouble() : doubleValue(bValue, bType);
        }

        aValue.setDouble(a * b);

      } else {

        BigDecimal a;
        BigDecimal b;

        if (sameType) {
          a = aValue.getBigDecimal();
          b = bValue.getBigDecimal();
        } else {
          a = aType == BIG_DECIMAL ?
            aValue.getBigDecimal() : bigDecimalValue(aValue, aType);
          b = bType == BIG_DECIMAL ?
            bValue.getBigDecimal() : bigDecimalValue(bValue, bType);
        }

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
      return isLessOrEqualThan(a, b) ? a : b;
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
      return isLessOrEqualThan(a, b) ? b : a;
    }

    /**
     * Compares two numerical property values and returns true,
     * if the first one is smaller.
     *
     * @param aValue first value
     * @param bValue second value
     *
     * @return a < b
     */
    private static boolean isLessOrEqualThan(
      PropertyValue aValue, PropertyValue bValue) {

      int aType = checkNumericalAndGetType(aValue);
      int bType = checkNumericalAndGetType(bValue);

      boolean sameType = aType == bType;

      int returnType = sameType ? aType : maxType(aType, bType);

      boolean aIsLessOrEqual;

      if (returnType == INT) {
        aIsLessOrEqual = aValue.getInt() <= bValue.getInt();

      } else if (returnType == FLOAT) {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : aValue.getInt();
          b = bType == FLOAT ? bValue.getFloat() : bValue.getInt();
        }

        aIsLessOrEqual = a <= b;

      } else if (returnType == LONG) {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : aValue.getInt();
          b = bType == LONG ? bValue.getLong() : bValue.getInt();
        }

        aIsLessOrEqual = a <= b;

      } else if (returnType == DOUBLE) {

        double a;
        double b;

        if (sameType) {
          a = aValue.getDouble();
          b = bValue.getDouble();
        } else {
          a = aType == DOUBLE ? aValue.getDouble() : doubleValue(aValue, aType);
          b = bType == DOUBLE ? bValue.getDouble() : doubleValue(bValue, bType);
        }

        aIsLessOrEqual = a <= b;

      } else {

        BigDecimal a;
        BigDecimal b;

        if (sameType) {
          a = aValue.getBigDecimal();
          b = bValue.getBigDecimal();
        } else {
          a = aType == BIG_DECIMAL ? aValue.getBigDecimal() :
            bigDecimalValue(aValue, aType);
          b = bType == BIG_DECIMAL ? bValue.getBigDecimal() :
            bigDecimalValue(bValue, bType);
        }

        aIsLessOrEqual = a.compareTo(b) <= 0;
      }

      return aIsLessOrEqual;
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
        return BigDecimal.valueOf(value.getLong());
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
  }
}
