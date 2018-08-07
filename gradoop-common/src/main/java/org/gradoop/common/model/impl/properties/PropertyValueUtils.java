/*
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

import org.gradoop.common.exceptions.UnsupportedTypeException;

import java.math.BigDecimal;
import java.util.Arrays;

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
     * Short type.
     */
    private static final int SHORT = 0;
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

      int returnType = maxType(aType, bType);

      if (returnType == INT)  {

        int a = aType == INT ? aValue.getInt() : aValue.getShort();
        int b = bType == INT ? bValue.getInt() : bValue.getShort();

        aValue.setInt(a + b);

      } else if (returnType == FLOAT)  {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : floatValue(aValue, aType);
          b = bType == FLOAT ? bValue.getFloat() : floatValue(bValue, bType);
        }

        aValue.setFloat(a + b);

      } else if (returnType == LONG)  {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : longValue(aValue, aType);
          b = bType == LONG ? bValue.getLong() : longValue(bValue, bType);
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
     * Returns the maximum of two types, at least Integer.
     *
     * @param aType first type
     * @param bType second type
     *
     * @return larger compatible type
     */
    private static int maxType(int aType, int bType) {
      return Math.max(Math.max(aType, bType), INT);
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

      int returnType = maxType(aType, bType);

      if (returnType == INT)  {

        int a = aType == INT ? aValue.getInt() : aValue.getShort();
        int b = bType == INT ? bValue.getInt() : bValue.getShort();

        aValue.setInt(a * b);

      } else if (returnType == FLOAT)  {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : floatValue(aValue, aType);
          b = bType == FLOAT ? bValue.getFloat() : floatValue(bValue, bType);
        }

        aValue.setFloat(a * b);

      } else if (returnType == LONG)  {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : longValue(aValue, aType);
          b = bType == LONG ? bValue.getLong() : longValue(bValue, bType);
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
     * Compares two numerical property values
     *
     * @param aValue first value
     * @param bValue second value
     *
     * @return 0 if a is equal to b, < 0 if a is less than b and > 0 if a is greater than b
     */
    public static int compare(PropertyValue aValue, PropertyValue bValue) {

      int aType = checkNumericalAndGetType(aValue);
      int bType = checkNumericalAndGetType(bValue);

      boolean sameType = aType == bType;

      int maxType = Math.max(aType, bType);

      int result;

      if (maxType == SHORT) {
        result = Short.compare(aValue.getShort(), bValue.getShort());

      } else if (maxType == INT)  {
        int a;
        int b;

        if (sameType) {
          a = aValue.getInt();
          b = bValue.getInt();
        } else {
          a = aType == INT ? aValue.getInt() : aValue.getShort();
          b = bType == INT ? bValue.getInt() : bValue.getShort();
        }

        result = Integer.compare(a, b);

      } else if (maxType == FLOAT) {
        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : floatValue(aValue, aType);
          b = bType == FLOAT ? bValue.getFloat() : floatValue(bValue, bType);
        }

        result = Float.compare(a, b);

      } else if (maxType == LONG) {
        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : longValue(aValue, aType);
          b = bType == LONG ? bValue.getLong() : longValue(bValue, bType);
        }

        result = Long.compare(a, b);

      } else if (maxType == DOUBLE) {
        double a;
        double b;

        if (sameType) {
          a = aValue.getDouble();
          b = bValue.getDouble();
        } else {
          a = aType == DOUBLE ? aValue.getDouble() : doubleValue(aValue, aType);
          b = bType == DOUBLE ? bValue.getDouble() : doubleValue(bValue, bType);
        }

        result = Double.compare(a, b);

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

        result = a.compareTo(b);
      }

      return result;
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

      int returnType = maxType(aType, bType);

      boolean aIsLessOrEqual;

      if (returnType == INT)  {

        int a = aType == INT ? aValue.getInt() : aValue.getShort();
        int b = bType == INT ? bValue.getInt() : bValue.getShort();

        aIsLessOrEqual = a <= b;

      } else if (returnType == FLOAT) {

        float a;
        float b;

        if (sameType) {
          a = aValue.getFloat();
          b = bValue.getFloat();
        } else {
          a = aType == FLOAT ? aValue.getFloat() : floatValue(aValue, aType);
          b = bType == FLOAT ? bValue.getFloat() : floatValue(bValue, bType);
        }

        aIsLessOrEqual = a <= b;

      } else if (returnType == LONG) {

        long a;
        long b;

        if (sameType) {
          a = aValue.getLong();
          b = bValue.getLong();
        } else {
          a = aType == LONG ? aValue.getLong() : longValue(aValue, aType);
          b = bType == LONG ? bValue.getLong() : longValue(bValue, bType);
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

      if (value.isShort()) {
        type = SHORT;
      } else if (value.isInt()) {
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
      case SHORT:
        return BigDecimal.valueOf(value.getShort());
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
      case SHORT:
        return value.getShort();
      case INT:
        return value.getInt();
      case LONG:
        return value.getLong();
      default:
        return value.getFloat();
      }
    }

    /**
     * Converts a value of a lower domain numerical type to Long.
     *
     * @param value value
     * @param type type
     *
     * @return converted value
     */
    private static long longValue(PropertyValue value, int type) {
      switch (type) {
      case SHORT:
        return value.getShort();
      default:
        return value.getInt();
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
      case SHORT:
        return value.getShort();
      case INT:
        return value.getInt();
      default:
        return value.getLong();
      }
    }
  }

  /**
   * Byte utilities.
   */
  public static class Bytes {

    /**
     * Get the raw byte representation of a {@link PropertyValue} instance without the type byte as prefix.
     *
     * @param value the {@link PropertyValue} to extract the bytes
     * @return a byte array containing the value without type information
     */
    public static byte[] getRawBytesWithoutType(PropertyValue value) {
      return Arrays.copyOfRange(value.getRawBytes(), 1, value.getRawBytes().length);
    }

    /**
     * Get the type byte of a {@link PropertyValue} instance. It's the first one of the
     * raw representation of a PropertyValue.
     *
     * @param value the {@link PropertyValue} to extract the type byte
     * @return the type byte as array
     */
    public static byte[] getTypeByte(PropertyValue value) {
      byte[] typeByte = new byte[1];
      typeByte[0] = value.getRawBytes()[0];
      return typeByte;
    }

    /**
     * Creates a {@link PropertyValue} instance by concatenating the byte representations
     * of the type and the value.
     *
     * @param typeByte a byte array containing only one byte representing the value type
     * @param valueBytes a byte array representing the property value
     * @return the resulting {@link PropertyValue}
     */
    public static PropertyValue createFromTypeValueBytes(byte[] typeByte, byte[] valueBytes) {
      byte[] validValue = new byte[valueBytes.length + 1];
      validValue[0] = typeByte[0];
      System.arraycopy(valueBytes, 0, validValue, 1, valueBytes.length);
      return PropertyValue.fromRawBytes(validValue);
    }
  }
}
