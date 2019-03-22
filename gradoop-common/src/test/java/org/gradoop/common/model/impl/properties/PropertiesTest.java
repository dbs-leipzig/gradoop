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
import com.google.common.collect.Maps;
import junitparams.JUnitParamsRunner;
import junitparams.NamedParameters;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
@RunWith(JUnitParamsRunner.class)
public class PropertiesTest {

  @Test
  @Parameters(named = "mapProvider, nullProvider")
  public void testCreateFromMap(Map<String, Object> input) {
    Properties properties = Properties.createFromMap(input);

    if (input == null) {
      assertEquals(properties.size(), 0);
    } else {
      assertEquals(input.size(), properties.size());

      for (Map.Entry<String, Object> entry : input.entrySet()) {
        String k = entry.getKey();
        assertTrue(properties.containsKey(k));
        assertEquals(entry.getValue(), properties.get(k).getObject());
      }
    }
  }

  @NamedParameters("mapProvider")
  private Object[] createFromMapDataProvider() {
    return new Map[]{
      SUPPORTED_PROPERTIES, // Map of all supported types
      Maps.newHashMap()     // Empty map
    };
  }

  @NamedParameters("nullProvider")
  private Object[] nullDataProvider() {
    return new Object[] { null };
  }

  @Test
  @Parameters(method = "getKeysDataProvider")
  public void testGetKeys(Properties input, Collection expectedKeys) {
    List<String> actualKeyList = Lists.newArrayList(input.getKeys());

    assertEquals(expectedKeys.size(), actualKeyList.size());
    for (String actualKey : actualKeyList) {
      assertTrue("key was not in key list", expectedKeys.contains(actualKey));
    }
  }

  private Object[] getKeysDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();

    return new Object[] {
      new Object[] {supportedProperties, Lists.newArrayList(SUPPORTED_PROPERTIES.keySet())},
      new Object[] {emptyProperties, Lists.newArrayList()}};
  }

  @Test
  @Parameters(method = "containsKeyDataProvider")
  public void testContainsKey(Properties input, String key, boolean expectedBoolean) {
    assertEquals("Input: " + input + "\nKey: " + key, expectedBoolean, input.containsKey(key));
  }

  private Object[] containsKeyDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();

    return new Object[] {
      new Object[] {supportedProperties, KEY_1, true},
      new Object[] {supportedProperties, "1234", false},
      new Object[] {emptyProperties, KEY_5, false},
      new Object[] {emptyProperties, "", false}};
  }

  @Test
  @Parameters(method = "getDataProvider")
  public void testGet(Properties input, String key, PropertyValue expectedValue) {
    assertEquals("Input: " + input + "\nKey: " + key, expectedValue, input.get(key));
  }

  private Object[] getDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    return new Object[] {
      new Object[] {supportedProperties, KEY_1, PropertyValue.create(BOOL_VAL_1)},
      new Object[] {supportedProperties, KEY_6, PropertyValue.create(STRING_VAL_6)},
      new Object[] {supportedProperties, KEY_0, PropertyValue.create(null)},
      new Object[] {supportedProperties, "1234", null}};
  }

  @Test
  @Parameters(method = "setPropertyDataProvider")
  public void testSetProperty(Property input, String key, Object expectedObject) {
    Properties properties = Properties.create();
    properties.set(input);

    assertEquals(expectedObject, properties.get(key).getObject());
  }

  private Object[] setPropertyDataProvider() {
    return new Object[] {
      new Object[] {Property.create(KEY_1, BOOL_VAL_1), KEY_1, BOOL_VAL_1},
      new Object[] {Property.create(KEY_7, BIG_DECIMAL_VAL_7), KEY_7, BIG_DECIMAL_VAL_7},
      new Object[] {Property.create("0", "some value"), "0", "some value"}};
  }

  @Test
  @Parameters(method = "setPropertyValueDataProvider")
  public void testSetPropertyValue(String inputKey, PropertyValue inputValue, Object expectedObj) {
    Properties properties = Properties.create();
    properties.set(inputKey, inputValue);

    assertEquals(expectedObj, properties.get(inputKey).getObject());
  }

  private Object[] setPropertyValueDataProvider() {
    return new Object[] {
      new Object[] {KEY_2, PropertyValue.create(INT_VAL_2), INT_VAL_2},
      new Object[] {KEY_8, PropertyValue.create(GRADOOP_ID_VAL_8), GRADOOP_ID_VAL_8},
      new Object[] {"", PropertyValue.create(null), null}};
  }

  @Test
  @Parameters(method = "setObjectDataProvider")
  public void testSetObject(String inputKey, Object inputObject) {
    Properties properties = Properties.create();
    properties.set(inputKey, inputObject);

    if (inputObject instanceof PropertyValue) {
      assertEquals(((PropertyValue) inputObject).getObject(), properties.get(inputKey).getObject());
    } else {
      assertEquals(inputObject, properties.get(inputKey).getObject());
    }
  }

  private Object[] setObjectDataProvider() {
    return new Object[] {
      new Object[] {"key", (short) 34},
      new Object[] {"key", BigDecimal.valueOf(533952L)},
      new Object[] {"null", null},
      new Object[] {"key", PropertyValue.create("This is a var length val.")}
    };
  }

  @Test
  @Parameters(method = "removeDataProvider")
  public void testRemoveByKey(Properties input, String key, int expectedSize, Object expectedValue) {
    PropertyValue removedValue = input.remove(key);

    assertEquals(expectedSize, input.size());
    assertEquals(expectedValue, removedValue);
  }

  private Object[] removeDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();

    return new Object[] {
      //            Input Properties obj | Key | Expected size after removing | Expected output
      new Object[] {supportedProperties, KEY_3, supportedProperties.size() - 1, PropertyValue.create(LONG_VAL_3)},
      new Object[] {emptyProperties, "key", 0, null}
    };
  }

  @Test
  @Parameters(method = "removeByPropertyDataProvider")
  public void testRemoveByProperty(Properties input, Property property, int expectedSize, PropertyValue expectedValue) {
    PropertyValue removedValue = input.remove(property);

    assertEquals(expectedSize, input.size());
    assertEquals(expectedValue, removedValue);
  }

  private Object[] removeByPropertyDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();
    Property floatProperty = Property.create(KEY_4, FLOAT_VAL_4);

    return new Object[] {
      //            Input Properties obj | Property | Expected size after removing | Expected output
      new Object[] {supportedProperties, floatProperty, supportedProperties.size() - 1, PropertyValue.create(FLOAT_VAL_4)},
      new Object[] {emptyProperties, floatProperty, 0, null}
    };
  }

  @Test
  public void testClear() {
    Properties properties = Properties.create();
    properties.set(KEY_1, BOOL_VAL_1);
    properties.clear();
    assertEquals("wrong size", 0, properties.size());
  }

  @Test
  public void testSize() {
    Properties properties = Properties.create();
    assertEquals("wrong size", 0, properties.size());
    properties.set(KEY_1, BOOL_VAL_1);
    assertEquals("wrong size", 1, properties.size());
    properties.set(KEY_2, INT_VAL_2);
    assertEquals("wrong size", 2, properties.size());
    // add existing
    properties.set(KEY_2, LONG_VAL_3);
    assertEquals("wrong size", 2, properties.size());
  }

  @Test
  public void testIsEmpty() {
    Properties properties = Properties.create();
    assertTrue("properties was not empty", properties.isEmpty());
    properties.set(KEY_1, BOOL_VAL_1);
    assertFalse("properties was empty", properties.isEmpty());
  }

  @Test
  public void testEqualsAndHashCode() {
    Properties properties1 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties properties2 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties properties3 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    // override property
    properties3.set(KEY_1, INT_VAL_2);

    assertEquals("properties were not equal", properties1, properties2);
    assertNotEquals("properties were equal", properties1, properties3);

    assertEquals("different hash code", properties1.hashCode(), properties2.hashCode());
    assertTrue("same hash code", properties1.hashCode() != properties3.hashCode());

    properties1 = Properties.create();
    properties1.set(KEY_1, BOOL_VAL_1);
    properties1.set(KEY_2, INT_VAL_2);

    properties2 = Properties.create();
    properties2.set(KEY_1, BOOL_VAL_1);

    assertNotEquals("properties were equal", properties1, properties2);
    assertTrue("same hash code", properties1.hashCode() != properties2.hashCode());
  }

  @Test
  @Parameters(method = "iteratorDataProvider")
  public void testIterator(Properties inputProperties, Map<String, PropertyValue> expectedValues) {
    assertNotNull(inputProperties.iterator());

    for (Property input : inputProperties) {
      assertTrue(expectedValues.containsKey(input.getKey()));
      assertEquals(expectedValues.get(input.getKey()), input.getValue().getObject());
    }
  }

  private Object[] iteratorDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();

    return new Object[] {
      new Object[] {supportedProperties, SUPPORTED_PROPERTIES},
      new Object[] {emptyProperties, new TreeMap<String, PropertyValue>()}
    };
  }

  @Test
  @Parameters(method = "writeAndReadFieldsDataProvider")
  public void testWriteAndReadFields(Properties input) throws Exception {
    Properties propertiesOut = writeAndReadFields(Properties.class, input);

    assertEquals(input, propertiesOut);
  }

  private Object[] writeAndReadFieldsDataProvider() {
    Properties supportedProperties = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties emptyProperties = Properties.create();
    // Create a large String. (Fill that String with copies of any char.)
    char[] stringData = new char[PropertyValue.LARGE_PROPERTY_THRESHOLD + 1];
    Arrays.fill(stringData, 'A');
    String largeString = new String(stringData);
    Properties largeProperties = Properties.create();
    largeProperties.set("large", largeString);

    return new Object[] {
      supportedProperties, emptyProperties, largeProperties
    };
  }
}
