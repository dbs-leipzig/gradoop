/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import com.google.common.collect.Lists;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.*;

public class PropertyValueTest {

  /**
   * Tests if {@link PropertyValue#create(Object)} works with supported types.
   */
  @Test
  public void testCreate() {
    // null
    PropertyValue p = create(null);
    assertTrue(p.isNull());
    assertNull(p.getObject());
    // boolean
    p = create(BOOL_VAL_1);
    assertTrue(p.isBoolean());
    assertEquals(BOOL_VAL_1, p.getBoolean());
    // short
    p = create(SHORT_VAL_e);
    assertTrue(p.isShort());
    assertEquals(SHORT_VAL_e, p.getShort());
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
    // GradoopId
    p = create(GRADOOP_ID_VAL_8);
    assertTrue(p.isGradoopId());
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
    // Map
    p = create(MAP_VAL_9);
    assertTrue(p.isMap());
    assertEquals(MAP_VAL_9, p.getMap());
    // List
    p = create(LIST_VAL_a);
    assertTrue(p.isList());
    assertEquals(LIST_VAL_a, p.getList());
    // Date
    p = create(DATE_VAL_b);
    assertTrue(p.isDate());
    assertEquals(DATE_VAL_b, p.getDate());
    // Time
    p = create(TIME_VAL_c);
    assertTrue(p.isTime());
    assertEquals(TIME_VAL_c, p.getTime());
    // DateTime
    p = create(DATETIME_VAL_d);
    assertTrue(p.isDateTime());
    assertEquals(DATETIME_VAL_d, p.getDateTime());
    // Set
    p = create(SET_VAL_f);
    assertTrue(p.isSet());
    assertEquals(SET_VAL_f, p.getSet());
  }

  /**
   * Test copying the property value
   */
  @Test
  public void testCopyBoolean() {
    PropertyValue p = create(BOOL_VAL_1);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a float type is wrapped.
   */
  @Test
  public void testCopyFloat() {
    PropertyValue p = create(FLOAT_VAL_4);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(FLOAT_VAL_4, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a double type is wrapped.
   */
  @Test
  public void testCopyDouble() {
    PropertyValue p = create(DOUBLE_VAL_5);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(DOUBLE_VAL_5, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link BigDecimal} type is wrapped.
   */
  @Test
  public void testCopyBigDecimal() {
    PropertyValue p = create(BIG_DECIMAL_VAL_7);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(BIG_DECIMAL_VAL_7, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link GradoopId} type is wrapped.
   */
  @Test
  public void testCopyGradoopId() {
    PropertyValue p = create(GRADOOP_ID_VAL_8);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(GRADOOP_ID_VAL_8, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link LocalDate} type is wrapped.
   */
  @Test
  public void testCopyDate() {
    PropertyValue p = create(DATE_VAL_b);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(DATE_VAL_b, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link LocalDateTime} type is wrapped.
   */
  @Test
  public void testCopyDateTime() {
    PropertyValue p = create(DATETIME_VAL_d);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(DATETIME_VAL_d, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link LocalTime} type is wrapped.
   */
  @Test
  public void testCopyTime() {
    PropertyValue p = create(TIME_VAL_c);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(TIME_VAL_c, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when an int type is wrapped.
   */
  @Test
  public void testCopyInteger() {
    PropertyValue p = create(INT_VAL_2);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link String} type is wrapped.
   */
  @Test
  public void testCopyString() {
    PropertyValue p = create(STRING_VAL_6);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(STRING_VAL_6, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link Map} type is wrapped.
   */
  @Test
  public void testCopyMap() {
    PropertyValue p = create(MAP_VAL_9);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(MAP_VAL_9, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link List} type is wrapped.
   */
  @Test
  public void testCopyList() {
    PropertyValue p = create(LIST_VAL_a);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(LIST_VAL_a, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue#copy()} works when a {@link Set} type is wrapped.
   */
  @Test
  public void testCopySet() {
    PropertyValue p = create(SET_VAL_f);
    PropertyValue copy = p.copy();
    assertEquals(p, copy);
    assertNotSame(p, copy);
    assertNotSame(SET_VAL_f, copy.getObject());
  }

  /**
   * Tests if {@link PropertyValue} setter and getter methods work as expected.
   * X -> p.setObject(X) -> p.getObject() -> X | where X equals X
   */
  @Test
  public void testSetAndGetObject() {
    PropertyValue p = new PropertyValue();
    // null
    p.setObject(null);
    assertTrue(p.isNull());
    assertNull(p.getObject());
    // boolean
    p.setObject(BOOL_VAL_1);
    assertTrue(p.isBoolean());
    assertEquals(BOOL_VAL_1, p.getObject());
    // short
    p.setObject(SHORT_VAL_e);
    assertTrue(p.isShort());
    assertEquals(SHORT_VAL_e, p.getObject());
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
    // GradoopId
    p.setObject(GRADOOP_ID_VAL_8);
    assertTrue(p.isGradoopId());
    assertEquals(GRADOOP_ID_VAL_8, p.getObject());
    // Map
    p.setObject(MAP_VAL_9);
    assertTrue(p.isMap());
    assertEquals(MAP_VAL_9, p.getObject());
    // List
    p.setObject(LIST_VAL_a);
    assertTrue(p.isList());
    assertEquals(LIST_VAL_a, p.getObject());
    // Date
    p.setObject(DATE_VAL_b);
    assertTrue(p.isDate());
    assertEquals(DATE_VAL_b, p.getDate());
    // Time
    p.setObject(TIME_VAL_c);
    assertTrue(p.isTime());
    assertEquals(TIME_VAL_c, p.getTime());
    // DateTime
    p.setObject(DATETIME_VAL_d);
    assertTrue(p.isDateTime());
    assertEquals(DATETIME_VAL_d, p.getDateTime());
    // Set
    p.setObject(SET_VAL_f);
    assertTrue(p.isSet());
    assertEquals(SET_VAL_f, p.getSet());
  }

  /**
   * Tests if {@link PropertyValue#setObject(Object)} throws an {@link UnsupportedTypeException} if
   * an unsupported type is passed as an argument.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    PropertyValue p = new PropertyValue();
    p.setObject(new PriorityQueue<>());
  }

  /**
   * Tests whether {@link PropertyValue#isNull()} returns true iff the instance wraps {@code null}.
   */
  @Test
  public void testIsNull() {
    PropertyValue p = PropertyValue.create(null);
    assertTrue(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests whether {@link PropertyValue#isBoolean()} returns true iff the instance wraps a
   * {@code boolean}.
   */
  @Test
  public void testIsBoolean() {
    PropertyValue p = PropertyValue.create(true);
    assertFalse(p.isNull());
    assertTrue(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getBoolean()}.
   */
  @Test
  public void testGetBoolean() {
    PropertyValue p = PropertyValue.create(BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, p.getBoolean());
  }

  /**
   * Tests {@link PropertyValue#setBoolean(boolean)}.
   */
  @Test
  public void testSetBoolean() {
    PropertyValue p = new PropertyValue();
    p.setBoolean(BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, p.getBoolean());
  }

  /**
   * Tests {@link PropertyValue#isShort()}.
   */
  @Test
  public void testIsShort() {
    PropertyValue p = PropertyValue.create(SHORT_VAL_e);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertTrue(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getShort()}.
   */
  @Test
  public void testGetShort() {
    PropertyValue p = PropertyValue.create(SHORT_VAL_e);
    assertEquals(SHORT_VAL_e, p.getShort());
  }

  /**
   * Tests {@link PropertyValue#setShort(short)}.
   */
  @Test
  public void testSetShort() {
    PropertyValue p = new PropertyValue();
    p.setShort(SHORT_VAL_e);
    assertEquals(SHORT_VAL_e, p.getShort());
  }

  /**
   * Tests {@link PropertyValue#isInt()}.
   */
  @Test
  public void testIsInt() {
    PropertyValue p = PropertyValue.create(INT_VAL_2);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertTrue(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getInt()}.
   */
  @Test
  public void testGetInt() {
    PropertyValue p = PropertyValue.create(INT_VAL_2);
    assertEquals(INT_VAL_2, p.getInt());
  }

  /**
   * Tests {@link PropertyValue#setInt(int)}.
   */
  @Test
  public void testSetInt() {
    PropertyValue p = new PropertyValue();
    p.setInt(INT_VAL_2);
    assertEquals(INT_VAL_2, p.getInt());
  }

  /**
   * Tests {@link PropertyValue#isLong()}.
   */
  @Test
  public void testIsLong() {
    PropertyValue p = PropertyValue.create(LONG_VAL_3);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertTrue(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getLong()}.
   */
  @Test
  public void testGetLong() {
    PropertyValue p = PropertyValue.create(LONG_VAL_3);
    assertEquals(LONG_VAL_3, p.getLong());
  }

  /**
   * Tests {@link PropertyValue#setLong(long)}.
   */
  @Test
  public void testSetLong()  {
    PropertyValue p = new PropertyValue();
    p.setLong(LONG_VAL_3);
    assertEquals(LONG_VAL_3, p.getLong());
  }

  /**
   * Tests {@link PropertyValue#isFloat()}.
   */
  @Test
  public void testIsFloat()  {
    PropertyValue p = PropertyValue.create(FLOAT_VAL_4);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertTrue(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getFloat()}.
   */
  @Test
  public void testGetFloat() {
    PropertyValue p = PropertyValue.create(FLOAT_VAL_4);
    assertEquals(FLOAT_VAL_4, p.getFloat(), 0);
  }

  /**
   * Tests {@link PropertyValue#setFloat(float)}.
   */
  @Test
  public void testSetFloat() {
    PropertyValue p = new PropertyValue();
    p.setFloat(FLOAT_VAL_4);
    assertEquals(FLOAT_VAL_4, p.getFloat(), 0);
  }

  /**
   * Tests {@link PropertyValue#isDouble()}.
   */
  @Test
  public void testIsDouble() {
    PropertyValue p = PropertyValue.create(DOUBLE_VAL_5);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertTrue(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getDouble()}.
   */
  @Test
  public void testGetDouble() {
    PropertyValue p = PropertyValue.create(DOUBLE_VAL_5);
    assertEquals(DOUBLE_VAL_5, p.getDouble(), 0);
  }

  /**
   * Tests {@link PropertyValue#setDouble(double)}.
   */
  @Test
  public void testSetDouble() {
    PropertyValue p = new PropertyValue();
    p.setDouble(DOUBLE_VAL_5);
    assertEquals(DOUBLE_VAL_5, p.getDouble(), 0);
  }

  /**
   * Tests {@link PropertyValue#isString()}.
   */
  @Test
  public void testIsString() {
    PropertyValue p = PropertyValue.create(STRING_VAL_6);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertTrue(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getString()}.
   */
  @Test
  public void testGetString() {
    PropertyValue p = PropertyValue.create(STRING_VAL_6);
    assertEquals(STRING_VAL_6, p.getString());
  }

  /**
   * Tests {@link PropertyValue#setString(String)}.
   */
  @Test
  public void testSetString() {
    PropertyValue p = new PropertyValue();
    p.setString(STRING_VAL_6);
    assertEquals(STRING_VAL_6, p.getString());
  }

  /**
   * Tests {@link PropertyValue#isBigDecimal()}.
   */
  @Test
  public void testIsBigDecimal() {
    PropertyValue p = PropertyValue.create(BIG_DECIMAL_VAL_7);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertTrue(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getBigDecimal()}.
   */
  @Test
  public void testGetBigDecimal() {
    PropertyValue p = PropertyValue.create(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  /**
   * Tests {@link PropertyValue#setBigDecimal(BigDecimal)}.
   */
  @Test
  public void testSetBigDecimal() {
    PropertyValue p = new PropertyValue();
    p.setBigDecimal(BIG_DECIMAL_VAL_7);
    assertEquals(BIG_DECIMAL_VAL_7, p.getBigDecimal());
  }

  /**
   * Tests {@link PropertyValue#isGradoopId()}.
   */
  @Test
  public void testIsGradoopId() {
    PropertyValue p = PropertyValue.create(GRADOOP_ID_VAL_8);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertTrue(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getGradoopId()}.
   */
  @Test
  public void testGetGradoopId() {
    PropertyValue p = PropertyValue.create(GRADOOP_ID_VAL_8);
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
  }

  /**
   * Tests {@link PropertyValue#setGradoopId(GradoopId)}.
   */
  @Test
  public void testSetGradoopId() {
    PropertyValue p = new PropertyValue();
    p.setGradoopId(GRADOOP_ID_VAL_8);
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
  }

  /**
   * Tests {@link PropertyValue#isMap()}.
   */
  @Test
  public void testIsMap() {
    PropertyValue p = PropertyValue.create(MAP_VAL_9);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertTrue(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getMap()}.
   */
  @Test
  public void testGetMap() {
    PropertyValue p = PropertyValue.create(MAP_VAL_9);
    assertEquals(MAP_VAL_9, p.getMap());
  }

  /**
   * Tests {@link PropertyValue#setMap(Map)}.
   */
  @Test
  public void testSetMap() {
    PropertyValue p = new PropertyValue();
    p.setMap(MAP_VAL_9);
    assertEquals(MAP_VAL_9, p.getMap());
  }

  /**
   * Tests {@link PropertyValue#isList()}.
   */
  @Test
  public void testIsList() {
    PropertyValue p = PropertyValue.create(LIST_VAL_a);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertTrue(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests whether passing a {@link List} which is not parametrized as {@link PropertyValue} to
   * {@link PropertyValue#create(Object)} will result in an {@link UnsupportedTypeException}.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void testCreateWrongParameterizedList() {
    List<String> list = new ArrayList<>();
    list.add("test1");
    list.add("test2");
    list.add("test3");
    PropertyValue p = PropertyValue.create(list);
  }

  /**
   * Tests whether passing a {@link Map} which is not parametrized as {@link PropertyValue} to
   * {@link PropertyValue#create(Object)} will result in an {@link UnsupportedTypeException}.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void testCreateWrongParameterizedMap() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "val1");
    map.put("key2", "val2");
    map.put("key3", "val3");
    PropertyValue p = PropertyValue.create(map);
  }

  /**
   * Tests whether passing a {@link Set} which is not parametrized as {@link PropertyValue} to
   * {@link PropertyValue#create(Object)} will result in an {@link UnsupportedTypeException}.
   */
  @Test(expected = UnsupportedTypeException.class)
  public void testCreateWrongParameterizedSet() {
    Set<String> set = new HashSet<>();
    set.add("test1");
    set.add("test2");
    set.add("test3");
    PropertyValue p = PropertyValue.create(set);
  }

  /**
   * Tests whether {@link PropertyValue#create(Object)} works when an empty {@link List} is
   * provided.
   */
  @Test
  public void testCreateEmptyList() {
    List<PropertyValue> list = new ArrayList<>();
    PropertyValue p = PropertyValue.create(list);
  }

  /**
   * Tests {@link PropertyValue#getList()}.
   */
  @Test
  public void testGetList() {
    PropertyValue p = PropertyValue.create(LIST_VAL_a);
    assertEquals(LIST_VAL_a, p.getList());
  }

  /**
   * Tests {@link PropertyValue#setList(List)}.
   */
  @Test
  public void testSetList() {
    PropertyValue p = new PropertyValue();
    p.setList(LIST_VAL_a);
    assertEquals(LIST_VAL_a, p.getList());
  }

  /**
   * Tests {@link PropertyValue#isDate()}.
   */
  @Test
  public void testIsDate() {
    PropertyValue p = PropertyValue.create(DATE_VAL_b);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertTrue(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getDate()}.
   */
  @Test
  public void testGetDate() {
    PropertyValue p = PropertyValue.create(DATE_VAL_b);
    assertEquals(DATE_VAL_b, p.getDate());
  }

  /**
   * Tests {@link PropertyValue#setDate(LocalDate)}.
   */
  @Test
  public void testSetDate() {
    PropertyValue p = new PropertyValue();
    p.setDate(DATE_VAL_b);
    assertEquals(DATE_VAL_b, p.getDate());
  }

  /**
   * Tests {@link PropertyValue#isTime()}.
   */
  @Test
  public void testIsTime() {
    PropertyValue p = PropertyValue.create(TIME_VAL_c);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertTrue(p.isTime());
    assertFalse(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * tests {@link PropertyValue#getTime()}.
   */
  @Test
  public void testGetTime() {
    PropertyValue p = PropertyValue.create(TIME_VAL_c);
    assertEquals(TIME_VAL_c, p.getTime());
  }

  /**
   * Tests {@link PropertyValue#setTime(LocalTime)}.
   */
  @Test
  public void testSetTime() {
    PropertyValue p = new PropertyValue();
    p.setTime(TIME_VAL_c);
    assertEquals(TIME_VAL_c, p.getTime());
  }

  /**
   * Tests {@link PropertyValue#isDateTime()}.
   */
  @Test
  public void testIsDateTime() {
    PropertyValue p = PropertyValue.create(DATETIME_VAL_d);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertTrue(p.isDateTime());
    assertFalse(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getDateTime()}.
   */
  @Test
  public void testGetDateTime() {
    PropertyValue p = PropertyValue.create(DATETIME_VAL_d);
    assertEquals(DATETIME_VAL_d, p.getDateTime());
  }

  /**
   * Tests {@link PropertyValue#setDateTime(LocalDateTime)}.
   */
  @Test
  public void testSetDateTime() {
    PropertyValue p = new PropertyValue();
    p.setDateTime(DATETIME_VAL_d);
    assertEquals(DATETIME_VAL_d, p.getDateTime());
  }

  /**
   * Tests {@link PropertyValue#isSet()}.
   */
  @Test
  public void testIsSet() {
    PropertyValue p = PropertyValue.create(SET_VAL_f);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
    assertFalse(p.isShort());
    assertFalse(p.isInt());
    assertFalse(p.isLong());
    assertFalse(p.isFloat());
    assertFalse(p.isDouble());
    assertFalse(p.isString());
    assertFalse(p.isBigDecimal());
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
    assertTrue(p.isSet());
  }

  /**
   * Tests {@link PropertyValue#getSet()}.
   */
  @Test
  public void testGetSet() {
    PropertyValue p = PropertyValue.create(SET_VAL_f);
    assertEquals(SET_VAL_f, p.getSet());
  }

  /**
   * Tests {@link PropertyValue#setSet(Set)}.
   */
  @Test
  public void testSetSet() {
    PropertyValue p = new PropertyValue();
    p.setSet(SET_VAL_f);
    assertEquals(SET_VAL_f, p.getSet());
  }

  /**
   * Tests {@link PropertyValue#isNumber()}.
   */
  @Test
  public void testIsNumber() {
    PropertyValue p = PropertyValue.create(SHORT_VAL_e);
    assertTrue(p.isNumber());
    p = PropertyValue.create(INT_VAL_2);
    assertTrue(p.isNumber());
    p = PropertyValue.create(LONG_VAL_3);
    assertTrue(p.isNumber());
    p = PropertyValue.create(FLOAT_VAL_4);
    assertTrue(p.isNumber());
    p = PropertyValue.create(DOUBLE_VAL_5);
    assertTrue(p.isNumber());
    p = PropertyValue.create(BIG_DECIMAL_VAL_7);
    assertTrue(p.isNumber());

    p = PropertyValue.create(NULL_VAL_0);
    assertFalse(p.isNumber());
    p = PropertyValue.create(BOOL_VAL_1);
    assertFalse(p.isNumber());
    p = PropertyValue.create(STRING_VAL_6);
    assertFalse(p.isNumber());
    p = PropertyValue.create(GRADOOP_ID_VAL_8);
    assertFalse(p.isNumber());
    p = PropertyValue.create(MAP_VAL_9);
    assertFalse(p.isNumber());
    p = PropertyValue.create(LIST_VAL_a);
    assertFalse(p.isNumber());
    p = PropertyValue.create(DATE_VAL_b);
    assertFalse(p.isNumber());
    p = PropertyValue.create(TIME_VAL_c);
    assertFalse(p.isNumber());
    p = PropertyValue.create(DATETIME_VAL_d);
    assertFalse(p.isNumber());
    p = PropertyValue.create(SET_VAL_f);
    assertFalse(p.isNumber());
  }

  /**
   * Tests {@link PropertyValue#equals(Object)} and {@link PropertyValue#hashCode()}.
   */
  @Test
  public void testEqualsAndHashCode() {
    validateEqualsAndHashCode(create(null), create(null), create(false));

    validateEqualsAndHashCode(create(true), create(true), create(false));

    validateEqualsAndHashCode(create((short) 10), create((short) 10), create((short) 11));

    validateEqualsAndHashCode(create(10), create(10), create(11));

    validateEqualsAndHashCode(create(10L), create(10L), create(11L));

    validateEqualsAndHashCode(create(10F), create(10F), create(11F));

    validateEqualsAndHashCode(create(10.), create(10.), create(11.));

    validateEqualsAndHashCode(create("10"), create("10"), create("11"));

    validateEqualsAndHashCode(create(new BigDecimal(10)),
      create(new BigDecimal(10)),
      create(new BigDecimal(11)));

    validateEqualsAndHashCode(
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a9099a"))
    );

    Map<PropertyValue, PropertyValue> map1 = new HashMap<>();
    map1.put(PropertyValue.create("foo"), PropertyValue.create("bar"));
    Map<PropertyValue, PropertyValue> map2 = new HashMap<>();
    map2.put(PropertyValue.create("foo"), PropertyValue.create("bar"));
    Map<PropertyValue, PropertyValue> map3 = new HashMap<>();
    map3.put(PropertyValue.create("foo"), PropertyValue.create("baz"));
    validateEqualsAndHashCode(create(map1), create(map2), create(map3));

    List<PropertyValue> list1 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("bar")
    );
    List<PropertyValue> list2 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("bar")
    );
    List<PropertyValue> list3 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("baz")
    );
    validateEqualsAndHashCode(create(list1), create(list2), create(list3));

    LocalDate date1 = LocalDate.MAX;
    LocalDate date2 = LocalDate.MAX;
    LocalDate date3 = LocalDate.now();

    validateEqualsAndHashCode(create(date1), create(date2), create(date3));

    LocalTime time1 = LocalTime.MAX;
    LocalTime time2 = LocalTime.MAX;
    LocalTime time3 = LocalTime.now();

    validateEqualsAndHashCode(create(time1), create(time2), create(time3));

    LocalDateTime dateTime1 = LocalDateTime.of(date1, time1);
    LocalDateTime dateTime2 = LocalDateTime.of(date2, time2);
    LocalDateTime dateTime3 = LocalDateTime.of(date3, time3);

    validateEqualsAndHashCode(create(dateTime1), create(dateTime2), create(dateTime3));

    Set<PropertyValue> set1 = new HashSet<>();
    set1.add(PropertyValue.create("bar"));
    Set<PropertyValue> set2 = new HashSet<>();
    set2.add(PropertyValue.create("bar"));
    Set<PropertyValue> set3 = new HashSet<>();
    set3.add(PropertyValue.create("baz"));
    validateEqualsAndHashCode(create(set1), create(set2), create(set3));
  }

  private void validateEqualsAndHashCode(PropertyValue p1, PropertyValue p2, PropertyValue p3) {
    assertEquals(p1, p1);
    assertEquals(p1, p2);
    assertNotEquals(p1, p3);

    assertEquals(p1.hashCode(), p1.hashCode());
    assertEquals(p1.hashCode(), p2.hashCode());
    assertNotEquals(p1.hashCode(), p3.hashCode());
  }

  /**
   * Tests whether an instance of {@link PropertyValue} which was created with {@code null} equals
   * {@code null}.
   */
  @Test
  public void testEqualsWithNull() {
    PropertyValue p = PropertyValue.create(null);
    assertNotEquals(p, null);
  }

  /**
   * Tests {@link PropertyValue#compareTo(PropertyValue)}.
   */
  @Test
  public void testCompareTo() {
    // null
    assertEquals(create(null).compareTo(create(null)), 0);
    // boolean
    validateCompareTo(create(false), create(false), create(true));
    // short
    validateCompareTo(create((short) -10), create((short) -10), create((short) 12));
    validateCompareTo(create((short) 10), create((short) 10), create((short) 12));
    validateCompareTo(create((short) -10), create(-10), create(12));
    validateCompareTo(create((short) 10), create(10), create(12));
    validateCompareTo(create((short) -10), create(-10L), create(12L));
    validateCompareTo(create((short) 10), create(10L), create(12L));
    validateCompareTo(create((short) -10), create(-10F), create(12F));
    validateCompareTo(create((short) 10), create(10F), create(12F));
    validateCompareTo(create((short) -10), create(-10D), create(12D));
    validateCompareTo(create((short) 10), create(10D), create(12D));
    validateCompareTo(create((short) -10), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create((short) 10), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // int
    validateCompareTo(create(-10), create((short) -10), create((short) 12));
    validateCompareTo(create(10), create((short) 10), create((short) 12));
    validateCompareTo(create(-10), create(-10), create(12));
    validateCompareTo(create(10), create(10), create(12));
    validateCompareTo(create(-10), create(-10L), create(12L));
    validateCompareTo(create(10), create(10L), create(12L));
    validateCompareTo(create(-10), create(-10F), create(12F));
    validateCompareTo(create(10), create(10F), create(12F));
    validateCompareTo(create(-10), create(-10D), create(12D));
    validateCompareTo(create(10), create(10D), create(12D));
    validateCompareTo(create(-10), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create(10), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // long
    validateCompareTo(create(-10L), create((short) -10), create((short) 12));
    validateCompareTo(create(10L), create((short) 10), create((short) 12));
    validateCompareTo(create(-10L), create(-10), create(12));
    validateCompareTo(create(10L), create(10), create(12));
    validateCompareTo(create(-10L), create(-10L), create(12L));
    validateCompareTo(create(10L), create(10L), create(12L));
    validateCompareTo(create(-10L), create(-10F), create(12F));
    validateCompareTo(create(10L), create(10F), create(12F));
    validateCompareTo(create(-10L), create(-10D), create(12D));
    validateCompareTo(create(10L), create(10D), create(12D));
    validateCompareTo(create(-10L), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create(10L), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // float
    validateCompareTo(create(-10F), create((short) -10), create((short) 12));
    validateCompareTo(create(10F), create((short) 10), create((short) 12));
    validateCompareTo(create(-10F), create(-10), create(12));
    validateCompareTo(create(10F), create(10), create(12));
    validateCompareTo(create(-10F), create(-10L), create(12L));
    validateCompareTo(create(10F), create(10L), create(12L));
    validateCompareTo(create(-10F), create(-10F), create(12F));
    validateCompareTo(create(10F), create(10F), create(12F));
    validateCompareTo(create(-10F), create(-10D), create(12D));
    validateCompareTo(create(10F), create(10D), create(12D));
    validateCompareTo(create(-10F), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create(10F), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // double
    validateCompareTo(create(-10D), create((short) -10), create((short) 12));
    validateCompareTo(create(10D), create((short) 10), create((short) 12));
    validateCompareTo(create(-10D), create(-10), create(12));
    validateCompareTo(create(10D), create(10), create(12));
    validateCompareTo(create(-10D), create(-10L), create(12L));
    validateCompareTo(create(10D), create(10L), create(12L));
    validateCompareTo(create(-10D), create(-10F), create(12F));
    validateCompareTo(create(10D), create(10F), create(12F));
    validateCompareTo(create(-10D), create(-10D), create(12D));
    validateCompareTo(create(10D), create(10D), create(12D));
    validateCompareTo(create(-10D), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create(10D), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // string
    validateCompareTo(create("10"), create("10"), create("12"));
    // BigDecimal
    validateCompareTo(create(BigDecimal.valueOf(-10)), create((short) -10), create((short) 12));
    validateCompareTo(create(BigDecimal.valueOf(10)), create((short) 10), create((short) 12));
    validateCompareTo(create(BigDecimal.valueOf(-10)), create(-10), create(12));
    validateCompareTo(create(BigDecimal.valueOf(10)), create(10), create(12));
    validateCompareTo(create(BigDecimal.valueOf(-10)), create(-10L), create(12L));
    validateCompareTo(create(BigDecimal.valueOf(10)), create(10L), create(12L));
    validateCompareTo(create(BigDecimal.valueOf(-10)), create(-10F), create(12F));
    validateCompareTo(create(BigDecimal.valueOf(10)), create(10F), create(12F));
    validateCompareTo(create(BigDecimal.valueOf(-10)), create(-10D), create(12D));
    validateCompareTo(create(BigDecimal.valueOf(10)), create(10D), create(12D));
    validateCompareTo(create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(-10)), create(BigDecimal.valueOf(12)));
    validateCompareTo(create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(10)), create(BigDecimal.valueOf(12)));
    // GradoopId
    validateCompareTo(
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a9099a"))
    );
    // Date
    validateCompareTo(
      create(DATE_VAL_b),
      create(DATE_VAL_b),
      create(DATE_VAL_b.plusDays(1L))
    );
    // Time
    validateCompareTo(
      create(TIME_VAL_c),
      create(TIME_VAL_c),
      create(TIME_VAL_c.plusSeconds(1L))
    );
    // DateTime
    validateCompareTo(
      create(DATETIME_VAL_d),
      create(DATETIME_VAL_d),
      create(DATETIME_VAL_d.plusNanos(1L))
    );
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} when the instances types are incomparable.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testCompareToWithIncompatibleTypes() {
    create(10).compareTo(create("10"));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link Map}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompareToWithMap() {
    create(MAP_VAL_9).compareTo(create(MAP_VAL_9));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link List}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompareToWithList() {
    create(LIST_VAL_a).compareTo(create(LIST_VAL_a));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link Set}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCompareToWithSet() {
    create(SET_VAL_f).compareTo(create(SET_VAL_f));
  }

  /**
   * Tests {@link PropertyValue#setBytes(byte[])}.
   */
  @Test
  public void testArrayValueMaxSize() {
    PropertyValue property = new PropertyValue();
    property.setBytes(new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD]);
  }

  /**
   * Tests {@link PropertyValue#setBytes(byte[])} with value greater than {@link Short#MAX_VALUE}.
   */
  @Test
  public void testLargeArrayValue() {
    PropertyValue property = new PropertyValue();
    property.setBytes(new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD + 1]);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with large string.
   */
  @Test
  public void testStringValueMaxSize() {
    create(new String(new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD]));
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with string larger than {@link Short#MAX_VALUE}.
   */
  @Test
  public void testLargeString() {
    create(new String(new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD + 10]));
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with big {@link List}.
   */
  @Test
  public void testListValueMaxSize() {
    int n = PropertyValue.LARGE_PROPERTY_THRESHOLD / 9;
    List<PropertyValue> list = new ArrayList<>(n);
    while (n-- > 0) {
      list.add(create(Math.random()));
    }
    create(list);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with {@link List} of
   * length > {@link Short#MAX_VALUE}.
   */
  @Test
  public void testLargeListValue() {
    // 8 bytes per double + 1 byte overhead
    int n = PropertyValue.LARGE_PROPERTY_THRESHOLD / 9 + 1;
    List<PropertyValue> list = new ArrayList<>(n);
    while (n-- > 0) {
      list.add(create(Math.random()));
    }
    create(list);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with big {@link Map}.
   */
  @Test
  public void testMapValueMaxSize() {
    Map<PropertyValue, PropertyValue> m = new HashMap<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.LARGE_PROPERTY_THRESHOLD / 18; i++) {
      PropertyValue p = create(Math.random());
      m.put(p, p);
    }
    create(m);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with {@link Map} of
   * size > {@link Short#MAX_VALUE}.
   */
  @Test
  public void testLargeMapValue() {
    Map<PropertyValue, PropertyValue> m = new HashMap<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.LARGE_PROPERTY_THRESHOLD / 18 + 1; i++) {
      PropertyValue p = create(Math.random());
      m.put(p, p);
    }
    create(m);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with big {@link Set}.
   */
  @Test
  public void testSetValueMaxSize() {
    Set<PropertyValue> s = new HashSet<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.LARGE_PROPERTY_THRESHOLD / 9; i++) {
      PropertyValue p = create(Math.random());
      s.add(p);
    }
    create(s);
  }

  /**
   * Tests {@link PropertyValue#create(Object)} with {@link Set} of
   * size > {@link Short#MAX_VALUE}.
   */
  @Test
  public void testLargeSetValue() {
    Set<PropertyValue> s = new HashSet<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.LARGE_PROPERTY_THRESHOLD / 9 + 1; i++) {
      PropertyValue p = create(Math.random());
      s.add(p);
    }
    create(s);
  }

  /**
   * Tests {@link PropertyValue#create} with big {@link BigDecimal}.
   */
  @Test
  public void testBigDecimalValueMaxSize() {
    // internal representation of BigInteger needs 5 bytes
    byte [] bigendian = new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD];
    Arrays.fill(bigendian, (byte) 121);
    create(new BigDecimal(new BigInteger(bigendian)));
  }

  /**
   * Tests {@link PropertyValue#create} large {@link BigDecimal}.
   */
  @Test
  public void testLargeBigDecimal() {
    byte [] bigendian = new byte[Short.MAX_VALUE + 10];
    Arrays.fill(bigendian, (byte) 121);
    create(new BigDecimal(new BigInteger(bigendian)));
  }

  /**
   * Tests {@link PropertyValue#write(DataOutputView)} and
   * {@link PropertyValue#read(DataInputView)}.
   *
   * @throws IOException if something goes wrong.
   */
  @Test
  public void testWriteAndReadFields() throws IOException {
    PropertyValue p = create(NULL_VAL_0);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(BOOL_VAL_1);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(SHORT_VAL_e);
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

    p = create(GRADOOP_ID_VAL_8);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(MAP_VAL_9);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(LIST_VAL_a);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(DATE_VAL_b);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(TIME_VAL_c);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(DATETIME_VAL_d);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));

    p = create(SET_VAL_f);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }

  /**
   * Tests that {@link PropertyValue#read(DataInputView)} and
   * {@link PropertyValue#write(DataOutputView)} can handle large {@link String} values.
   *
   * @throws Exception when something goes wrong.
   */
  @Test
  public void testReadAndWriteLargeString() throws Exception {
    PropertyValue p = create(new String(new byte[PropertyValue.LARGE_PROPERTY_THRESHOLD]));
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }

  /**
   * Tests that {@link PropertyValue#read(DataInputView)} and
   * {@link PropertyValue#write(DataOutputView)} can handle large {@link BigDecimal} values.
   *
   * @throws Exception when something goes wrong.
   */
  @Test
  public void testReadAndWriteLargeBigDecimal() throws Exception {
    byte [] bigEndian = new byte[Short.MAX_VALUE + 10];
    Arrays.fill(bigEndian, (byte) 121);
    PropertyValue p = create(new BigDecimal(new BigInteger(bigEndian)));
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }

  /**
   * Tests that {@link PropertyValue#read(DataInputView)} and
   * {@link PropertyValue#write(DataOutputView)} can handle large {@link Map} values.
   *
   * @throws Exception when something goes wrong.
   */
  @Test
  public void testReadAndWriteLargeMap() throws Exception {
    HashMap<PropertyValue, PropertyValue> largeMap = new HashMap<>();
    long neededEntries = PropertyValue.LARGE_PROPERTY_THRESHOLD / 10;
    for (int i = 0; i < neededEntries; i++) {
      largeMap.put(PropertyValue.create("key" + i), PropertyValue.create("value" + i));
    }

    PropertyValue p = create(largeMap);
    assertEquals(p, writeAndReadFields(PropertyValue.class, p));
  }

  /**
   * Tests {@link PropertyValue#getType()}.
   */
  @Test
  public void testGetType() {
    PropertyValue p = create(NULL_VAL_0);
    assertNull(p.getType());

    p = create(BOOL_VAL_1);
    assertEquals(Boolean.class, p.getType());

    p = create(SHORT_VAL_e);
    assertEquals(Short.class, p.getType());

    p = create(INT_VAL_2);
    assertEquals(Integer.class, p.getType());

    p = create(LONG_VAL_3);
    assertEquals(Long.class, p.getType());

    p = create(FLOAT_VAL_4);
    assertEquals(Float.class, p.getType());

    p = create(DOUBLE_VAL_5);
    assertEquals(Double.class, p.getType());

    p = create(STRING_VAL_6);
    assertEquals(String.class, p.getType());

    p = create(BIG_DECIMAL_VAL_7);
    assertEquals(BigDecimal.class, p.getType());

    p = create(GRADOOP_ID_VAL_8);
    assertEquals(GradoopId.class, p.getType());

    p = create(MAP_VAL_9);
    assertEquals(Map.class, p.getType());

    p = create(LIST_VAL_a);
    assertEquals(List.class, p.getType());

    p = create(DATE_VAL_b);
    assertEquals(LocalDate.class, p.getType());

    p = create(TIME_VAL_c);
    assertEquals(LocalTime.class, p.getType());

    p = create(DATETIME_VAL_d);
    assertEquals(LocalDateTime.class, p.getType());

    p = create(SET_VAL_f);
    assertEquals(Set.class, p.getType());
  }

  /**
   * Assumes that p1 == p2 < p3
   */
  private void validateCompareTo(PropertyValue p1, PropertyValue p2, PropertyValue p3) {
    assertEquals(0, p1.compareTo(p1));
    assertEquals(0, p1.compareTo(p2));
    assertEquals(0, p2.compareTo(p1));
    assertTrue(p1.compareTo(p3) < 0);
    assertTrue(p3.compareTo(p1) > 0);
    assertTrue(p3.compareTo(p2) > 0);
  }
}
