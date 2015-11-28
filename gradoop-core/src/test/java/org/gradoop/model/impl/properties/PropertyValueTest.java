package org.gradoop.model.impl.properties;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.gradoop.model.api.EPGMPropertyValue;
import org.gradoop.storage.exceptions.UnsupportedTypeException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class PropertyValueTest {

  @Test
  public void testCreate() throws Exception {
    // boolean
    EPGMPropertyValue p = PropertyValue.create(true);
    assertTrue(p.isBoolean());
    assertEquals(true, p.getBoolean());
    // int
    p = PropertyValue.create(10);
    assertTrue(p.isInt());
    assertEquals(10, p.getInt());
    // long
    p = PropertyValue.create(10L);
    assertTrue(p.isLong());
    assertEquals(10L, p.getLong());
    // float
    p = PropertyValue.create(10f);
    assertTrue(p.isFloat());
    assertEquals(10f, p.getFloat(), 0);
    // double
    p = PropertyValue.create(10.);
    assertTrue(p.isDouble());
    assertEquals(10., p.getDouble(), 0);
    // String
    p = PropertyValue.create("10");
    assertTrue(p.isString());
    assertEquals("10", p.getString());
    // BigDecimal
    p = PropertyValue.create(new BigDecimal(10L));
    assertTrue(p.isBigDecimal());
    assertEquals(new BigDecimal(10L), p.getBigDecimal());
    // DateTime
    DateTime now = DateTime.now();
    p = PropertyValue.create(now);
    assertTrue(p.isDateTime());
    assertEquals(now, p.getDateTime());
  }

  @Test
  public void testSetObject() throws Exception {
    EPGMPropertyValue p = new PropertyValue();

    p.setObject(true);
    assertTrue(p.isBoolean());
    assertEquals(true, p.getBoolean());
    // int
    p.setObject(10);
    assertTrue(p.isInt());
    assertEquals(10, p.getInt());
    // long
    p.setObject(10L);
    assertTrue(p.isLong());
    assertEquals(10L, p.getLong());
    // float
    p.setObject(10f);
    assertTrue(p.isFloat());
    assertEquals(10f, p.getFloat(), 0);
    // double
    p.setObject(10.);
    assertTrue(p.isDouble());
    assertEquals(10., p.getDouble(), 0);
    // String
    p.setObject("10");
    assertTrue(p.isString());
    assertEquals("10", p.getString());
    // BigDecimal
    p.setObject(new BigDecimal(10L));
    assertTrue(p.isBigDecimal());
    assertEquals(new BigDecimal(10L), p.getBigDecimal());
    // DateTime
    DateTime now = DateTime.now();
    p.setObject(now);
    assertTrue(p.isDateTime());
    assertEquals(now, p.getDateTime());
  }

  @Test(expected = NullPointerException.class)
  public void testSetObjectNull() {
    EPGMPropertyValue p = new PropertyValue();
    p.setObject(null);
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    EPGMPropertyValue p = new PropertyValue();
    p.setObject(new ArrayList<>());
  }

  @Test
  public void testIsBoolean() throws Exception {
    EPGMPropertyValue p = new PropertyValue(true);
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
    EPGMPropertyValue p = new PropertyValue(true);
    assertEquals(true, p.getBoolean());
  }

  @Test
  public void testSetBoolean() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setBoolean(true);
    assertEquals(true, p.getBoolean());
    p.setBoolean(false);
    assertEquals(false, p.getBoolean());
  }

  @Test
  public void testIsInt() throws Exception {
    EPGMPropertyValue p = new PropertyValue(10);
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
    EPGMPropertyValue p = new PropertyValue(10);
    assertEquals(10, p.getInt());
  }

  @Test
  public void testSetInt() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setInt(10);
    assertEquals(10, p.getInt());
  }

  @Test
  public void testIsLong() throws Exception {
    EPGMPropertyValue p = new PropertyValue(10L);
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
    EPGMPropertyValue p = new PropertyValue(10L);
    assertEquals(10L, p.getLong());
  }

  @Test
  public void testSetLong() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setLong(10L);
    assertEquals(10, p.getLong());
  }

  @Test
  public void testIsFloat() throws Exception {
    EPGMPropertyValue p = new PropertyValue(10f);
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
    EPGMPropertyValue p = new PropertyValue(10f);
    assertEquals(10f, p.getFloat(), 0);
  }

  @Test
  public void testSetFloat() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setFloat(10f);
    assertEquals(10f, p.getFloat(), 0);
  }

  @Test
  public void testIsDouble() throws Exception {
    EPGMPropertyValue p = new PropertyValue(10.);
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
    EPGMPropertyValue p = new PropertyValue(10.);
    assertEquals(10., p.getDouble(), 0);
  }

  @Test
  public void testSetDouble() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setDouble(10.);
    assertEquals(10., p.getDouble(), 0);
  }

  @Test
  public void testIsString() throws Exception {
    EPGMPropertyValue p = new PropertyValue("10");
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
    EPGMPropertyValue p = new PropertyValue("10");
    assertEquals("10", p.getString());
  }

  @Test
  public void testSetString() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setString("10");
    assertEquals("10", p.getString());
  }

  @Test
  public void testIsBigDecimal() throws Exception {
    EPGMPropertyValue p = new PropertyValue(new BigDecimal(10L));
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
    EPGMPropertyValue p = new PropertyValue(new BigDecimal(10L));
    assertEquals(new BigDecimal(10L), p.getBigDecimal());
  }

  @Test
  public void testSetBigDecimal() throws Exception {
    EPGMPropertyValue p = new PropertyValue();
    p.setBigDecimal(new BigDecimal(10L));
    assertEquals(new BigDecimal(10L), p.getBigDecimal());
  }

  @Test
  public void testIsDateTime() throws Exception {
    EPGMPropertyValue p = new PropertyValue(DateTime.now());
    assertFalse(p.isBoolean());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertTrue(p.isDateTime());
  }

  @Test
  public void testGetDateTime() throws Exception {
    DateTime now = DateTime.now();
    EPGMPropertyValue p = new PropertyValue(now);
    assertEquals(now, p.getDateTime());
  }

  @Test
  public void testSetDateTime() throws Exception {
    DateTime now = DateTime.now();
    EPGMPropertyValue p = new PropertyValue();
    p.setDateTime(now);
    assertEquals(now, p.getDateTime());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    validateEqualsAndHashCode(PropertyValue.create(true),
      PropertyValue.create(true), PropertyValue.create(false));

    validateEqualsAndHashCode(PropertyValue.create(10),
      PropertyValue.create(10), PropertyValue.create(11));

    validateEqualsAndHashCode(PropertyValue.create(10L),
      PropertyValue.create(10L), PropertyValue.create(11L));

    validateEqualsAndHashCode(PropertyValue.create(10F),
      PropertyValue.create(10F), PropertyValue.create(11F));

    // breaks for Double because of
    // https://issues.apache.org/jira/browse/HADOOP-12217

    validateEqualsAndHashCode(PropertyValue.create("10"),
      PropertyValue.create("10"), PropertyValue.create("11"));

    validateEqualsAndHashCode(PropertyValue.create(new BigDecimal(10)),
      PropertyValue.create(new BigDecimal(10)),
      PropertyValue.create(new BigDecimal(11)));

    DateTime now1 = new DateTime("2015-11-28T16:27:29.840+01:00");
    DateTime now2 = new DateTime("2015-11-28T16:27:30.840+01:00");
    validateEqualsAndHashCode(PropertyValue.create(now1),
      PropertyValue.create(now1), PropertyValue.create(now2));
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
    validateCompareTo(PropertyValue.create(false), PropertyValue.create(false),
      PropertyValue.create(true));

    validateCompareTo(PropertyValue.create(10), PropertyValue.create(10),
      PropertyValue.create(11));

    validateCompareTo(PropertyValue.create(10L), PropertyValue.create(10L),
      PropertyValue.create(11L));

    validateCompareTo(PropertyValue.create(10F), PropertyValue.create(10F),
      PropertyValue.create(11F));

    validateCompareTo(PropertyValue.create(10.), PropertyValue.create(10.),
      PropertyValue.create(11.));

    validateCompareTo(PropertyValue.create("10"), PropertyValue.create("10"),
      PropertyValue.create("11"));

    validateCompareTo(PropertyValue.create(new BigDecimal(10)),
      PropertyValue.create(new BigDecimal(10)),
      PropertyValue.create(new BigDecimal(11)));

    DateTime now1 = new DateTime("2015-11-28T16:27:29.840+01:00");
    DateTime now2 = new DateTime("2015-11-28T16:27:30.840+01:00");
    validateCompareTo(PropertyValue.create(now1), PropertyValue.create(now1),
      PropertyValue.create(now2));
  }

  @Test
  public void testWriteAndReadFields() throws IOException {
    PropertyValue p = PropertyValue.create(true);
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create(10);
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create(10L);
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create(10f);
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create(10.);
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create("10");
    assertEquals(p, writeAndReadFields(p));

    p = PropertyValue.create(new BigDecimal(10L));
    assertEquals(p, writeAndReadFields(p));

    // TODO: fails because of wrong ISOCHRONOLOGY after serializing
//    p = PropertyValue.create(new DateTime());
//    assertEquals(p, writeAndReadFields(p));
  }

  private PropertyValue writeAndReadFields(PropertyValue pIn) throws
    IOException {
    // write to byte[]
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    pIn.write(dataOut);

    // read from byte[]
    PropertyValue pOut = new PropertyValue();
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DataInputStream dataIn = new DataInputStream(in);
    pOut.readFields(dataIn);

    return pOut;
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