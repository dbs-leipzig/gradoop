package org.gradoop.common.model.impl.properties;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.*;

public class PropertyValueTest {

  @Test
  public void testCreate() throws Exception {
    // null
    PropertyValue p = create(null);
    assertTrue(p.isNull());
    assertNull(p.getObject());

    // boolean
    p = create(BOOL_VAL_1);
    assertTrue(p.isBoolean());
    assertEquals(BOOL_VAL_1, p.getBoolean());
    // int
    p = create(INT_VAL_2);
    assertTrue(p.isInt());
    assertEquals(INT_VAL_2, p.getInt());
    // long
    p = create(LONG_VAL_3);
    assertTrue(p.isLong());
    assertEquals(LONG_VAL_3, p.getLong());
    // float
    p = create(FLOAT_VAL_4);
    assertTrue(p.isFloat());
    assertEquals(FLOAT_VAL_4, p.getFloat(), 0);
    // double
    p = create(DOUBLE_VAL_5);
    assertTrue(p.isDouble());
    assertEquals(DOUBLE_VAL_5, p.getDouble(), 0);
    // String
    p = create(STRING_VAL_6);
    assertTrue(p.isString());
    assertEquals(STRING_VAL_6, p.getString());
    // BigDecimal
    p = create(BIG_DECIMAL_VAL_7);
    assertTrue(p.isBigDecimal());
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  @Test
  public void testSetAndGetObject() throws Exception {
    PropertyValue p = new PropertyValue();

    p.setObject(null);
    assertTrue(p.isNull());
    assertNull(p.getObject());

    p.setObject(BOOL_VAL_1);
    assertTrue(p.isBoolean());
    assertEquals(BOOL_VAL_1, p.getObject());
    // int
    p.setObject(INT_VAL_2);
    assertTrue(p.isInt());
    assertEquals(INT_VAL_2, p.getObject());
    // long
    p.setObject(LONG_VAL_3);
    assertTrue(p.isLong());
    assertEquals(LONG_VAL_3, p.getObject());
    // float
    p.setObject(FLOAT_VAL_4);
    assertTrue(p.isFloat());
    assertEquals(FLOAT_VAL_4, p.getObject());
    // double
    p.setObject(DOUBLE_VAL_5);
    assertTrue(p.isDouble());
    assertEquals(DOUBLE_VAL_5, p.getObject());
    // String
    p.setObject(STRING_VAL_6);
    assertTrue(p.isString());
    assertEquals(STRING_VAL_6, p.getObject());
    // BigDecimal
    p.setObject(BIG_DECIMAL_VAL_7);
    assertTrue(p.isBigDecimal());
    assertEquals(BIG_DECIMAL_VAL_7, p.getObject());
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    PropertyValue p = new PropertyValue();
    p.setObject(new ArrayList<>());
  }

  @Test
  public void testIsNull() throws Exception {
    PropertyValue p = PropertyValue.create(null);
    assertTrue(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testIsBoolean() throws Exception {
    PropertyValue p = PropertyValue.create(true);
    assertFalse(p.isNull());
    assertTrue(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetBoolean() throws Exception {
    PropertyValue p = PropertyValue.create(BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, p.getBoolean());
  }

  @Test
  public void testSetBoolean() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setBoolean(BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, p.getBoolean());
  }

  @Test
  public void testIsInt() throws Exception {
    PropertyValue p = PropertyValue.create(INT_VAL_2);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertTrue(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetInt() throws Exception {
    PropertyValue p = PropertyValue.create(INT_VAL_2);
    assertEquals(INT_VAL_2, p.getInt());
  }

  @Test
  public void testSetInt() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setInt(INT_VAL_2);
    assertEquals(INT_VAL_2, p.getInt());
  }

  @Test
  public void testIsLong() throws Exception {
    PropertyValue p = PropertyValue.create(LONG_VAL_3);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertTrue(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetLong() throws Exception {
    PropertyValue p = PropertyValue.create(LONG_VAL_3);
    assertEquals(LONG_VAL_3, p.getLong());
  }

  @Test
  public void testSetLong() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setLong(LONG_VAL_3);
    assertEquals(LONG_VAL_3, p.getLong());
  }

  @Test
  public void testIsFloat() throws Exception {
    PropertyValue p = PropertyValue.create(FLOAT_VAL_4);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertTrue(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetFloat() throws Exception {
    PropertyValue p = PropertyValue.create(FLOAT_VAL_4);
    assertEquals(FLOAT_VAL_4, p.getFloat(), 0);
  }

  @Test
  public void testSetFloat() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setFloat(FLOAT_VAL_4);
    assertEquals(FLOAT_VAL_4, p.getFloat(), 0);
  }

  @Test
  public void testIsDouble() throws Exception {
    PropertyValue p = PropertyValue.create(DOUBLE_VAL_5);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertTrue(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetDouble() throws Exception {
    PropertyValue p = PropertyValue.create(DOUBLE_VAL_5);
    assertEquals(DOUBLE_VAL_5, p.getDouble(), 0);
  }

  @Test
  public void testSetDouble() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setDouble(DOUBLE_VAL_5);
    assertEquals(DOUBLE_VAL_5, p.getDouble(), 0);
  }

  @Test
  public void testIsString() throws Exception {
    PropertyValue p = PropertyValue.create(STRING_VAL_6);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertTrue(p.isString());
    assertFalse(p.isBigDecimal());
  }

  @Test
  public void testGetString() throws Exception {
    PropertyValue p = PropertyValue.create(STRING_VAL_6);
    assertEquals(STRING_VAL_6, p.getString());
  }

  @Test
  public void testSetString() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setString(STRING_VAL_6);
    assertEquals(STRING_VAL_6, p.getString());
  }

  @Test
  public void testIsBigDecimal() throws Exception {
    PropertyValue p = PropertyValue.create(BIG_DECIMAL_VAL_7);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertTrue(p.isBigDecimal());
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    PropertyValue p = PropertyValue.create(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  @Test
  public void testSetBigDecimal() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setBigDecimal(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    validateEqualsAndHashCode(create(null), create(null), create(false));

    validateEqualsAndHashCode(create(true), create(true), create(false));

    validateEqualsAndHashCode(create(10), create(10), create(11));

    validateEqualsAndHashCode(create(10L), create(10L), create(11L));

    validateEqualsAndHashCode(create(10F), create(10F), create(11F));

    validateEqualsAndHashCode(create(10.), create(10.), create(11.));

    validateEqualsAndHashCode(create("10"), create("10"), create("11"));

    validateEqualsAndHashCode(create(new BigDecimal(10)),
      create(new BigDecimal(10)),
      create(new BigDecimal(11)));
  }

  /**
   * Assumes that p1.equals(p2) and !p2.equals(p3)
   */
  private void validateEqualsAndHashCode(PropertyValue p1, PropertyValue p2,
    PropertyValue p3) {
    assertEquals(p1, p1);
    assertEquals(p1, p2);
    assertNotEquals(p1, p3);

    assertTrue(p1.hashCode() == p1.hashCode());
    assertTrue(p1.hashCode() == p2.hashCode());
    assertFalse(p1.hashCode() == p3.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    assertTrue(create(null).compareTo(create(null)) == 0);

    validateCompareTo(create(false), create(false), create(true));

    validateCompareTo(create(-10), create(-10), create(10));
    validateCompareTo(create(10), create(10), create(12));

    validateCompareTo(create(-10L), create(-10L), create(12L));
    validateCompareTo(create(10L), create(10L), create(12L));

    validateCompareTo(create(-10F), create(-10F), create(12F));
    validateCompareTo(create(10F), create(10F), create(12F));

    validateCompareTo(create(-10.), create(-10.), create(12.));
    validateCompareTo(create(10.), create(10.), create(12.));

    validateCompareTo(create("10"), create("10"), create("12"));

    validateCompareTo(create(new BigDecimal(-10)),
      create(new BigDecimal(-10)),
      create(new BigDecimal(11)));
    validateCompareTo(create(new BigDecimal(10)),
      create(new BigDecimal(10)),
      create(new BigDecimal(11)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareToWithIncompatibleTypes() {
    create(10).compareTo(create(10L));
  }

  @Test
  public void testWriteAndReadFields() throws IOException {
    PropertyValue p = create(NULL_VAL_0);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(BOOL_VAL_1);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(INT_VAL_2);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(LONG_VAL_3);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(FLOAT_VAL_4);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(DOUBLE_VAL_5);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(STRING_VAL_6);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(BIG_DECIMAL_VAL_7);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }
  /**
   * Assumes that p1 == p2 < p3
   */
  private void validateCompareTo(PropertyValue p1, PropertyValue p2,
    PropertyValue p3) {
    assertTrue(p1.compareTo(p1) == 0);
    assertTrue(p1.compareTo(p2) == 0);
    assertTrue(p2.compareTo(p1) == 0);
    assertTrue(p1.compareTo(p3) < 0);
    assertTrue(p3.compareTo(p1) > 0);
    assertTrue(p3.compareTo(p2) > 0);
  }
}