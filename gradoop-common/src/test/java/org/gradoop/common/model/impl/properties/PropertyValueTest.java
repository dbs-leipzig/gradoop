/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.*;

public class PropertyValueTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testBigDecimalConversion() {
    PropertyValue property;
    BigDecimal decimalValue;

    // INT
    property = create(INT_VAL_2);
    decimalValue = BigDecimal.valueOf(INT_VAL_2);
    assertEquals(decimalValue, property.getBigDecimal());

    // LONG
    property = create(LONG_VAL_3);
    decimalValue = BigDecimal.valueOf(LONG_VAL_3);
    assertEquals(decimalValue, property.getBigDecimal());

    // FLOAT
    property = create(FLOAT_VAL_4);
    decimalValue = BigDecimal.valueOf(FLOAT_VAL_4);
    assertEquals(decimalValue, property.getBigDecimal());

    // DOUBLE
    property = create(DOUBLE_VAL_5);
    decimalValue = BigDecimal.valueOf(DOUBLE_VAL_5);
    assertEquals(decimalValue, property.getBigDecimal());

    // STRING
    property = create("-3.5");
    decimalValue = new BigDecimal("-3.5");
    assertEquals(decimalValue, property.getBigDecimal());

    exception.expect(NumberFormatException.class);
    property = create("No Number");
    property.getBigDecimal();
  }

  @Test
  public void testFailedBigDecimalConversion() {
    exception.expect(ClassCastException.class);
    create(BOOL_VAL_1).getBigDecimal();
  }

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
    //GradoopId
    p = create(GRADOOP_ID_VAL_8);
    assertTrue(p.isGradoopId());
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
    //Map
    p = create(MAP_VAL_9);
    assertTrue(p.isMap());
    assertEquals(MAP_VAL_9, p.getMap());
    //List
    p = create(LIST_VAL_a);
    assertTrue(p.isList());
    assertEquals(LIST_VAL_a, p.getList());
    //Date
    p = create(DATE_VAL_b);
    assertTrue(p.isDate());
    assertEquals(DATE_VAL_b, p.getDate());
    //Time
    p = create(TIME_VAL_c);
    assertTrue(p.isTime());
    assertEquals(TIME_VAL_c, p.getTime());
    //DateTime
    p = create(DATETIME_VAL_d);
    assertTrue(p.isDateTime());
    assertEquals(DATETIME_VAL_d, p.getDateTime());
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
    //Date
    p.setObject(DATE_VAL_b);
    assertTrue(p.isDate());
    assertEquals(DATE_VAL_b, p.getDate());
    //Time
    p.setObject(TIME_VAL_c);
    assertTrue(p.isTime());
    assertEquals(TIME_VAL_c, p.getTime());
    //DateTime
    p.setObject(DATETIME_VAL_d);
    assertTrue(p.isDateTime());
    assertEquals(DATETIME_VAL_d, p.getDateTime());
  }

  @Test(expected = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    PropertyValue p = new PropertyValue();
    p.setObject(new HashSet<>());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
    assertFalse(p.isGradoopId());
    assertFalse(p.isMap());
    assertFalse(p.isList());
    assertFalse(p.isDate());
    assertFalse(p.isTime());
    assertFalse(p.isDateTime());
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
  public void testIsGradoopId() throws Exception {
    PropertyValue p = PropertyValue.create(GRADOOP_ID_VAL_8);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetGradoopId() throws Exception {
    PropertyValue p = PropertyValue.create(GRADOOP_ID_VAL_8);
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
  }

  @Test
  public void testSetGradoopId() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setGradoopId(GRADOOP_ID_VAL_8);
    assertEquals(GRADOOP_ID_VAL_8, p.getGradoopId());
  }

  @Test
  public void testIsMap() throws Exception {
    PropertyValue p = PropertyValue.create(MAP_VAL_9);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetMap() throws Exception {
    PropertyValue p = PropertyValue.create(MAP_VAL_9);
    assertEquals(MAP_VAL_9, p.getMap());
  }

  @Test
  public void testSetMap() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setMap(MAP_VAL_9);
    assertEquals(MAP_VAL_9, p.getMap());
  }

  @Test
  public void testIsList() throws Exception {
    PropertyValue p = PropertyValue.create(LIST_VAL_a);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetList() throws Exception {
    PropertyValue p = PropertyValue.create(LIST_VAL_a);
    assertEquals(LIST_VAL_a, p.getList());
  }

  @Test
  public void testSetList() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setList(LIST_VAL_a);
    assertEquals(LIST_VAL_a, p.getList());
  }

  @Test
  public void testIsDate() throws Exception {
    PropertyValue p = PropertyValue.create(DATE_VAL_b);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetDate() throws Exception {
    PropertyValue p = PropertyValue.create(DATE_VAL_b);
    assertEquals(DATE_VAL_b, p.getDate());
  }

  @Test
  public void testSetDate() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setDate(DATE_VAL_b);
    assertEquals(DATE_VAL_b, p.getDate());
  }

  @Test
  public void testIsTime() throws Exception {
    PropertyValue p = PropertyValue.create(TIME_VAL_c);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetTime() throws Exception {
    PropertyValue p = PropertyValue.create(TIME_VAL_c);
    assertEquals(TIME_VAL_c, p.getTime());
  }

  @Test
  public void testSetTime() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setTime(TIME_VAL_c);
    assertEquals(TIME_VAL_c, p.getTime());
  }


  @Test
  public void testIsDateTime() throws Exception {
    PropertyValue p = PropertyValue.create(DATETIME_VAL_d);
    assertFalse(p.isNull());
    assertFalse(p.isBoolean());
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
  }

  @Test
  public void testGetDateTime() throws Exception {
    PropertyValue p = PropertyValue.create(DATETIME_VAL_d);
    assertEquals(DATETIME_VAL_d, p.getDateTime());
  }

  @Test
  public void testSetDateTime() throws Exception {
    PropertyValue p = new PropertyValue();
    p.setDateTime(DATETIME_VAL_d);
    assertEquals(DATETIME_VAL_d, p.getDateTime());
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
  }

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

    validateCompareTo(
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
      create(GradoopId.fromString("583ff8ffbd7d222690a9099a"))
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCompareToWithIncompatibleTypes() {
    create(10).compareTo(create(10L));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCompareToWithMap() {
    create(MAP_VAL_9).compareTo(create(MAP_VAL_9));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCompareToWithList() {
    create(LIST_VAL_a).compareTo(create(LIST_VAL_a));
  }

  @Test
  public void testArrayValueMaxSize() {
    PropertyValue property = new PropertyValue();
    property.setBytes(new byte[PropertyValue.MAX_BINARY_LENGTH]);
  }
  
  @Test(expected = IllegalStateException.class)
  public void testArrayValueTooBig() {
    PropertyValue property = new PropertyValue();
    property.setBytes(new byte[PropertyValue.MAX_BINARY_LENGTH + 1]);
  }

  @Test
  public void testStringValueMaxSize() {
    create(new String(new byte[PropertyValue.MAX_BINARY_LENGTH - 1]));
  }
  
  @Test(expected = IllegalStateException.class)
  public void testStringValueTooBig() {
    create(new String(new byte[PropertyValue.MAX_BINARY_LENGTH]));
  }
  
  @Test
  public void testListValueMaxSize() {
    int n = PropertyValue.MAX_BINARY_LENGTH / 9;
    List<PropertyValue> list = new ArrayList<>(n);
    while ( n-- > 0 ){
      list.add(create(Math.random()));
    }
    create(list);
  }
  
  @Test(expected = IllegalStateException.class)
  public void testListValueTooBig() {
    // 8 bytes per double + 1 byte overhead
    int n = PropertyValue.MAX_BINARY_LENGTH / 9 + 1;
    List<PropertyValue> list = new ArrayList<>(n);
    while ( n-- > 0 ){
      list.add(create(Math.random()));
    }
    create(list);
  }
  
  @Test
  public void testMapValueMaxSize() {
    Map<PropertyValue, PropertyValue> m = new HashMap<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.MAX_BINARY_LENGTH / 18; i++) {
      PropertyValue p = create(Math.random());
      m.put(p, p);
    }
    create(m);
  }
  
  @Test(expected = IllegalStateException.class)
  public void testMapValueTooBig() {
    Map<PropertyValue, PropertyValue> m = new HashMap<>();
    // 8 bytes per double + 1 byte overhead
    for (int i = 0; i < PropertyValue.MAX_BINARY_LENGTH / 18 + 1; i++) {
      PropertyValue p = create(Math.random());
      m.put(p, p);
    }
    create(m);
  }
  
  @Test
  public void testBigDecimalValueMaxSize() {
    // internal representation of BigInteger needs 5 bytes
    byte [] bigendian = new byte[PropertyValue.MAX_BINARY_LENGTH - 5];
    Arrays.fill(bigendian, (byte) 121);
    create(new BigDecimal(new BigInteger(bigendian)));
  }
  
  @Test(expected = IllegalStateException.class)
  public void testBigDecimalValueTooBig() {
    byte [] bigendian = new byte[PropertyValue.MAX_BINARY_LENGTH - 4];
    Arrays.fill(bigendian, (byte) 121);
    create(new BigDecimal(new BigInteger(bigendian)));
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