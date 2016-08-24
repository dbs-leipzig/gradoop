package org.gradoop.common.model.impl.properties;

import org.gradoop.common.storage.exceptions.UnsupportedTypeException;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkNotNull;

public class PropertyValues {

  private static final int INT = 1;
  private static final int LONG = 2;
  private static final int FLOAT = 3;
  private static final int DOUBLE = 4;
  private static final int BIG_DECIMAL = 5;

  public static PropertyValue add(PropertyValue aValue, PropertyValue bValue) {

    int aType = getType(aValue);
    int bType = getType(bValue);

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

  private static BigDecimal bigDecimalValue(PropertyValue value, int type) {
    switch (type) {
    case INT: return BigDecimal.valueOf(value.getInt());
    case LONG: return BigDecimal.valueOf(value.getDouble());
    case FLOAT: return BigDecimal.valueOf(value.getFloat());
    default: return BigDecimal.valueOf(value.getDouble());
    }
  }

  private static double doubleValue(PropertyValue value, int type) {
    switch (type) {
    case INT: return value.getInt();
    case LONG: return value.getLong();
    default: return value.getFloat();
    }
  }

  private static float floatValue(PropertyValue value, int type) {
    switch (type) {
    case INT: return value.getInt();
    default: return value.getLong();
    }
  }

  private static int getType(PropertyValue value) {
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

  public static PropertyValue min(PropertyValue a, PropertyValue b) {
    return isLessThan(a, b) ? a : b;
  }

  public static PropertyValue max(PropertyValue a, PropertyValue b) {
    return isLessThan(a, b) ? b : a;
  }

  private static boolean isLessThan(PropertyValue a, PropertyValue b) {
    boolean aIsSmaller;
    int aType = getType(a);
    int bType = getType(b);

    if (aType == bType) {
      switch (aType) {
      case BIG_DECIMAL: aIsSmaller =
        a.getBigDecimal().compareTo(b.getBigDecimal()) <= 0;
        break;
      case DOUBLE: aIsSmaller = a.getDouble() <= b.getDouble();
        break;
      case FLOAT: aIsSmaller = a.getFloat() <= b.getFloat();
        break;
      case LONG: aIsSmaller = a.getLong() <= b.getLong();
        break;
      default: aIsSmaller = a.getInt() <= b.getInt();
      }
    } else if (aType > bType) {
      switch (aType) {
      case BIG_DECIMAL: aIsSmaller =
        a.getBigDecimal().compareTo(bigDecimalValue(b, bType)) <= 0;
        break;
      case DOUBLE: aIsSmaller = a.getDouble() <= doubleValue(b, bType);
        break;
      case FLOAT: aIsSmaller = a.getFloat() <= floatValue(b, bType);
        break;
      default: aIsSmaller = a.getLong() <= b.getInt();
        break;
      }
    } else {
      switch (bType) {
      case BIG_DECIMAL: aIsSmaller =
        b.getBigDecimal().compareTo(bigDecimalValue(a, aType)) <= 0;
        break;
      case DOUBLE: aIsSmaller = b.getDouble() > doubleValue(a, aType);
        break;
      case FLOAT: aIsSmaller = b.getFloat() > floatValue(a, aType);
        break;
      default: aIsSmaller = b.getLong() > a.getInt();
        break;
      }
    }
    return aIsSmaller;
  }
}
