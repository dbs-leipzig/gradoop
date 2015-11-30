package org.gradoop.model.impl.properties;

import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import static org.gradoop.GradoopTestUtils.*;
import static org.gradoop.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.*;

public class PropertyValueTest {

  @Test
  public void testCreate() throws Exception {
    // boolean
    PropertyValue p = create(BOOL_VAL_1);
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
    // DateTime
    // TODO: supported when https://issues.apache.org/jira/browse/PIG-4748 is solved
//    p = create(DATETIME_VAL_8);
//    assertTrue(p.isDateTime());
//    assertEquals(DATETIME_VAL_8, p.getDateTime());
  }

  @Test
  public void testSetAndGetObject() throws Exception {
    PropertyValue p = new PropertyValue();

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
    // TODO: supported when https://issues.apache.org/jira/browse/PIG-4748 is solved
//    // DateTime
//    p.setObject(DATETIME_VAL_8);
//    assertTrue(p.isDateTime());
//    assertEquals(DATETIME_VAL_8, p.getObject());
  }

  @Test(expected = NullPointerException.class)
  public void testSetObjectNull() {
    PropertyValue p = new PropertyValue();
    p.setObject(null);
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    PropertyValue p = new PropertyValue();
    p.setObject(new ArrayList<>());
  }

  @Test
  public void testIsBoolean() throws Exception {
    PropertyValue p = new PropertyValue(true);
    assertTrue(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetBoolean() throws Exception {
    PropertyValue p = new PropertyValue(BOOL_VAL_1);
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
    PropertyValue p = new PropertyValue(INT_VAL_2);
    assertFalse(p.isBoolean());
    assertTrue(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetInt() throws Exception {
    PropertyValue p = new PropertyValue(INT_VAL_2);
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
    PropertyValue p = new PropertyValue(LONG_VAL_3);
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertTrue(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetLong() throws Exception {
    PropertyValue p = new PropertyValue(LONG_VAL_3);
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
    PropertyValue p = new PropertyValue(FLOAT_VAL_4);
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertTrue(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetFloat() throws Exception {
    PropertyValue p = new PropertyValue(FLOAT_VAL_4);
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
    PropertyValue p = new PropertyValue(DOUBLE_VAL_5);
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertTrue(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetDouble() throws Exception {
    PropertyValue p = new PropertyValue(DOUBLE_VAL_5);
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
    PropertyValue p = new PropertyValue(STRING_VAL_6);
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertTrue(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetString() throws Exception {
    PropertyValue p = new PropertyValue(STRING_VAL_6);
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
    PropertyValue p = new PropertyValue(BIG_DECIMAL_VAL_7);
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertTrue(p.isBigDecimal());
    assertFalse(p.isDateTime());
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    PropertyValue p = new PropertyValue(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  @Test
  public void testSetBigDecimal() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setBigDecimal(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  // TODO: supported when https://issues.apache.org/jira/browse/PIG-4748 is solved

//  @Test
//  public void testIsDateTime() throws Exception {
//    PropertyValue p = new PropertyValue(DATETIME_VAL_8);
//    assertFalse(p.isBoolean());
//    assertFalse(p.isInt());
//    assertFalse(p.isLong());
//    assertFalse(p.isFloat());
//    assertFalse(p.isDouble());
//    assertFalse(p.isString());
//    assertFalse(p.isBigDecimal());
//    assertTrue(p.isDateTime());
//  }
//
//  @Test
//  public void testGetDateTime() throws Exception {
//    PropertyValue p = new PropertyValue(DATETIME_VAL_8);
//    assertEquals(DATETIME_VAL_8, p.getDateTime());
//  }
//
//  @Test
//  public void testSetDateTime() throws Exception {
//    PropertyValue p = new PropertyValue();
//    p.setDateTime(DATETIME_VAL_8);
//    assertEquals(DATETIME_VAL_8, p.getDateTime());
//  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    validateEqualsAndHashCode(create(true), create(true), create(false));

    validateEqualsAndHashCode(create(10), create(10), create(11));

    validateEqualsAndHashCode(create(10L), create(10L), create(11L));

    validateEqualsAndHashCode(create(10F), create(10F), create(11F));

    // TODO: breaks for Double because of
    // https://issues.apache.org/jira/browse/HADOOP-12217
//    validateEqualsAndHashCode(create(10.), create(10.), create(11.));

    validateEqualsAndHashCode(create("10"), create("10"), create("11"));

    validateEqualsAndHashCode(create(new BigDecimal(10)),
      create(new BigDecimal(10)),
      create(new BigDecimal(11)));

    DateTime now1 = new DateTime("2015-11-28T16:27:29.840+01:00");
    DateTime now2 = new DateTime("2015-11-28T16:27:30.840+01:00");
    validateEqualsAndHashCode(create(now1), create(now1), create(now2));
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
    validateCompareTo(create(false), create(false),
      create(true));

    validateCompareTo(create(10), create(10),
      create(11));

    validateCompareTo(create(10L), create(10L),
      create(11L));

    validateCompareTo(create(10F), create(10F),
      create(11F));

    validateCompareTo(create(10.), create(10.),
      create(11.));

    validateCompareTo(create("10"), create("10"),
      create("11"));

    validateCompareTo(create(new BigDecimal(10)),
      create(new BigDecimal(10)),
      create(new BigDecimal(11)));

    DateTime now1 = new DateTime("2015-11-28T16:27:29.840+01:00");
    DateTime now2 = new DateTime("2015-11-28T16:27:30.840+01:00");
    validateCompareTo(create(now1), create(now1),
      create(now2));
  }

  @Test
  public void testWriteAndReadFields() throws IOException {
    PropertyValue p = create(BOOL_VAL_1);
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

    // TODO: fails because of wrong ISOCHRONOLOGY after serializing
//    p = PropertyValue.create(DATETIME_VAL_8);
//    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }
  /**
   * Assumes that p1 == p2 < p3
   */
  private void validateCompareTo(PropertyValue p1, PropertyValue p2,
    PropertyValue p3) {
    assertEquals(0, p1.compareTo(p1));
    assertEquals(0, p1.compareTo(p2));
    assertEquals(0, p2.compareTo(p1));
    assertEquals(-1, p1.compareTo(p3));
    assertEquals(1, p3.compareTo(p1));
    assertEquals(1, p3.compareTo(p2));
  }
}