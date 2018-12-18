package org.gradoop.common.model.impl.properties.strategies;

import org.gradoop.common.exceptions.UnsupportedTypeException;
import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

class PropertyValueStrategyUtils {

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

  static int compareNumerical(Number aValue, Number bValue) {

    int aType = getType(aValue);
    int bType = getType(bValue);

    boolean sameType = aType == bType;

    int maxType = Math.max(aType, bType);

    int result;

    if (maxType == SHORT) {
      result = Short.compare(aValue.shortValue(), bValue.shortValue());

    } else if (maxType == INT)  {
      int a;
      int b;

      if (sameType) {
        a = aValue.intValue();
        b = bValue.intValue();
      } else {
        a = aType == INT ? aValue.intValue() : aValue.shortValue();
        b = bType == INT ? bValue.intValue() : bValue.shortValue();
      }

      result = Integer.compare(a, b);

    } else if (maxType == FLOAT) {
      float a;
      float b;

      if (sameType) {
        a = aValue.floatValue();
        b = bValue.floatValue();
      } else {
        a = aType == FLOAT ? aValue.floatValue() : floatValue(aValue, aType);
        b = bType == FLOAT ? bValue.floatValue() : floatValue(bValue, bType);
      }

      result = Float.compare(a, b);

    } else if (maxType == LONG) {
      long a;
      long b;

      if (sameType) {
        a = aValue.longValue();
        b = bValue.longValue();
      } else {
        a = aType == LONG ? aValue.longValue() : longValue(aValue, aType);
        b = bType == LONG ? bValue.longValue() : longValue(bValue, bType);
      }

      result = Long.compare(a, b);

    } else if (maxType == DOUBLE) {
      double a;
      double b;

      if (sameType) {
        a = aValue.doubleValue();
        b = bValue.doubleValue();
      } else {
        a = aType == DOUBLE ? aValue.doubleValue() : doubleValue(aValue, aType);
        b = bType == DOUBLE ? bValue.doubleValue() : doubleValue(bValue, bType);
      }

      result = Double.compare(a, b);

    } else {
      BigDecimal a;
      BigDecimal b;

      if (sameType) {
        a = (BigDecimal) aValue;
        b = (BigDecimal) bValue;
      } else {
        a = aType == BIG_DECIMAL ? (BigDecimal) aValue :
          bigDecimalValue(aValue, aType);
        b = bType == BIG_DECIMAL ? (BigDecimal) bValue :
          bigDecimalValue(bValue, bType);
      }

      result = a.compareTo(b);
    }

    return result;
  }

  private static int getType(Number value) {
    checkNotNull(value);

    int type;

    if (value instanceof Short) {
      type = SHORT;
    } else if (value instanceof Integer) {
      type = INT;
    } else if (value instanceof Long) {
      type = LONG;
    } else if (value instanceof Float) {
      type = FLOAT;
    } else if (value instanceof Double) {
      type = DOUBLE;
    } else if (value instanceof BigDecimal) {
      type = BIG_DECIMAL;
    } else {
      throw new UnsupportedTypeException(value.getClass());
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
  private static BigDecimal bigDecimalValue(Number value, int type) {
    switch (type) {
    case SHORT:
      return BigDecimal.valueOf(value.shortValue());
    case INT:
      return BigDecimal.valueOf(value.intValue());
    case LONG:
      return BigDecimal.valueOf(value.longValue());
    case FLOAT:
      return BigDecimal.valueOf(value.floatValue());
    default:
      return BigDecimal.valueOf(value.doubleValue());
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
  private static double doubleValue(Number value, int type) {
    switch (type) {
    case SHORT:
      return value.shortValue();
    case INT:
      return value.intValue();
    case LONG:
      return value.longValue();
    default:
      return value.floatValue();
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
  private static long longValue(Number value, int type) {
    if (type == SHORT) {
      return value.shortValue();
    }
    return value.intValue();
  }

  /**
   * Converts a value of a lower domain numerical type to Float.
   *
   * @param value value
   * @param type type
   *
   * @return converted value
   */
  private static float floatValue(Number value, int type) {
    switch (type) {
    case SHORT:
      return value.shortValue();
    case INT:
      return value.intValue();
    default:
      return value.longValue();
    }
  }
}
