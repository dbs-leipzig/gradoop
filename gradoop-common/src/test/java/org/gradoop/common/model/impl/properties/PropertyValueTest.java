/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.id.GradoopId;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.*;

public class PropertyValueTest {

  /**
   * Tests if any type conversion of the kind PropertyValue.getType == A to B with
   * PropertyValue.getB raises an {@link UnsupportedOperationException}.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class,
    dataProvider = "propertyValueProvider",
    dataProviderClass = PropertyValueTestProvider.class)
  public void testIfTypeConversionThrowsException(PropertyValue actual) {
    if (actual.getType() != BigDecimal.class) {
      actual.getBigDecimal();
    } else {
      actual.getDate();
    }
  }

  /**
   * Tests if {@link PropertyValue#create(Object)} works with supported types.
   */
  @Test(dataProvider = "supportedTypeProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testCreate(Object supportedType) {
    PropertyValue p = create(supportedType);

    if (supportedType != null) {
      assertTrue(p.is(supportedType.getClass()));
      assertEquals(supportedType, p.get(supportedType.getClass()));
    } else {
      assertTrue(p.isNull());
      assertNull(p.getObject());
    }
  }

  /**
   * Tests if {@link PropertyValue#copy()} works for every supported type.
   */
  @Test(dataProvider = "testIsProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testCopy(PropertyValue value, Object supportedType) {
    PropertyValue copy = value.copy();
    assertEquals(value, copy);
    assertNotSame(value, copy);
    if (value instanceof List || value instanceof Map || value instanceof Set) {
      assertNotSame(supportedType, copy.getObject());
    }
  }

  /**
   * Tests if {@link PropertyValue} setter and getter methods work as expected.
   * X -> p.setObject(X) -> p.getObject() -> X | where X equals X
   */
  @Test(dataProvider = "supportedTypeProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testSetAndGetObject(Object supportedType) {
    PropertyValue value = new PropertyValue();

    value.setObject(supportedType);
    if (supportedType != null) {
      assertTrue(value.is(supportedType.getClass()));
    } else {
      assertTrue(value.isNull());
    }
    assertEquals(supportedType, value.getObject());
  }

  /**
   * Tests if {@link PropertyValue#setObject(Object)} throws an {@link UnsupportedTypeException} if
   * an unsupported type is passed as an argument.
   */
  @Test(expectedExceptions = UnsupportedTypeException.class)
  public void testSetObjectWithUnsupportedType() {
    PropertyValue p = new PropertyValue();
    p.setObject(new PriorityQueue<>());
  }

  /**
   * Tests whether isX only returns true iff the given value represents type X.
   *
   * @param value a PropertyValue
   * @param expectedType the type which is represented by value
   */
  @Test(dataProvider = "testIsProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testIs(PropertyValue value, Object expectedType) {
    assertEquals(Objects.equals(expectedType, NULL_VAL_0), value.isNull());
    assertEquals(Objects.equals(expectedType, BOOL_VAL_1), value.isBoolean());
    assertEquals(Objects.equals(expectedType, INT_VAL_2), value.isInt());
    assertEquals(Objects.equals(expectedType, LONG_VAL_3), value.isLong());
    assertEquals(Objects.equals(expectedType, FLOAT_VAL_4), value.isFloat());
    assertEquals(Objects.equals(expectedType, DOUBLE_VAL_5), value.isDouble());
    assertEquals(Objects.equals(expectedType, STRING_VAL_6), value.isString());
    assertEquals(Objects.equals(expectedType, BIG_DECIMAL_VAL_7), value.isBigDecimal());
    assertEquals(Objects.equals(expectedType, GRADOOP_ID_VAL_8), value.isGradoopId());
    assertEquals(Objects.equals(expectedType, MAP_VAL_9), value.isMap());
    assertEquals(Objects.equals(expectedType, LIST_VAL_a), value.isList());
    assertEquals(Objects.equals(expectedType, DATE_VAL_b), value.isDate());
    assertEquals(Objects.equals(expectedType, TIME_VAL_c), value.isTime());
    assertEquals(Objects.equals(expectedType, DATETIME_VAL_d), value.isDateTime());
    assertEquals(Objects.equals(expectedType, SHORT_VAL_e), value.isShort());
    assertEquals(Objects.equals(expectedType, SET_VAL_f), value.isSet());
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
   * Tests whether passing a {@link List} which is not parametrized as {@link PropertyValue} to
   * {@link PropertyValue#create(Object)} will result in an {@link UnsupportedTypeException}.
   */
  @Test(expectedExceptions = UnsupportedTypeException.class)
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
  @Test(expectedExceptions = UnsupportedTypeException.class)
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
  @Test(expectedExceptions = UnsupportedTypeException.class)
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
  @Test(dataProvider = "testIsNumberProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testIsNumber(PropertyValue value, boolean expected) {
    assertEquals(expected, value.isNumber());
  }

  /**
   * Tests {@link PropertyValue#equals(Object)} and {@link PropertyValue#hashCode()}.
   */
  @Test(dataProvider = "testEqualsAndHashCodeProvider",
    dataProviderClass = PropertyValueTestProvider.class)
  public void testEqualsAndHashCode(PropertyValue value1, PropertyValue value2, PropertyValue value3) {
    validateEqualsAndHashCode(value1, value2, value3);
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
    validateCompareTo(create(null), create(null), create(12));
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
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCompareToWithIncompatibleTypes() {
    create(10).compareTo(create("10"));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link Map}.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testCompareToWithMap() {
    create(MAP_VAL_9).compareTo(create(MAP_VAL_9));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link List}.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testCompareToWithList() {
    create(LIST_VAL_a).compareTo(create(LIST_VAL_a));
  }

  /**
   * Tests whether {@link PropertyValue#compareTo(PropertyValue)} throws an
   * {@link IllegalArgumentException} if the instance is of {@link Set}.
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
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
  @Test(dataProvider = "propertyValueProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testWriteAndReadFields(PropertyValue value) throws IOException {
    assertEquals(value, writeAndReadFields(PropertyValue.class, value));
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
  @Test(dataProvider = "supportedTypeProvider", dataProviderClass = PropertyValueTestProvider.class)
  public void testGetType(Object supportedType) {
    PropertyValue value = create(supportedType);
    if (supportedType instanceof List || supportedType instanceof Map || supportedType instanceof Set) {
      assertEquals(supportedType.getClass().getInterfaces()[0], value.getType());
    } else if (supportedType != null) {
      assertEquals(supportedType.getClass(), value.getType());
    } else {
      assertNull(value.getType());
    }
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
