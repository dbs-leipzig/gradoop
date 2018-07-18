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

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
public class PropertiesTest {

  @Test
  public void testCreateFromMap() throws Exception {
    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    assertEquals(SUPPORTED_PROPERTIES.size(), properties.size());

    for (Map.Entry<String, Object> entry : SUPPORTED_PROPERTIES.entrySet()) {
      String k = entry.getKey();
      assertTrue(properties.containsKey(k));
      assertEquals(entry.getValue(), properties.get(k).getObject());
    }
  }

  @Test
  public void testGetKeys() throws Exception {
    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    List<String> keyList = Lists.newArrayList(properties.getKeys());

    assertEquals(SUPPORTED_PROPERTIES.size(), keyList.size());
    for (String expectedKey : SUPPORTED_PROPERTIES.keySet()) {
      assertTrue("key was not in key list", keyList.contains(expectedKey));
    }
  }

  @Test
  public void testContainsKey() throws Exception {
    Properties properties = Properties.create();

    assertFalse("unexpected key found", properties.containsKey(KEY_1));
    properties.set(KEY_1, BOOL_VAL_1);
    assertTrue("key not found", properties.containsKey(KEY_1));
    assertFalse("unexpected key found", properties.containsKey("1234"));
  }

  @Test
  public void testGet() throws Exception {
    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    assertNotNull("property was null", properties.get(KEY_1));
    assertEquals("wrong property", BOOL_VAL_1, properties.get(KEY_1).getBoolean());
    assertNull("unexpected property", properties.get("1234"));
  }

  @Test
  public void testSet() throws Exception {
    Properties properties = Properties.create();

    properties.set(Property.create(KEY_1, BOOL_VAL_1));
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(Property.create(KEY_1, INT_VAL_2));
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testSet1() throws Exception {
    Properties properties = Properties.create();

    properties.set(KEY_1, PropertyValue.create(BOOL_VAL_1));
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(KEY_1, PropertyValue.create(INT_VAL_2));
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testSet2() throws Exception {
    Properties properties = Properties.create();

    properties.set(KEY_1, BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(KEY_1, INT_VAL_2);
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testRemove() throws Exception {
    Properties properties = Properties.create();
    PropertyValue removed;

    properties.set(KEY_1, BOOL_VAL_1);
    removed = properties.remove(KEY_1);
    assertEquals(0, properties.size());
    assertNotNull(removed);

    properties.set(KEY_1, BOOL_VAL_1);
    removed = properties.remove(KEY_2);
    assertEquals(1, properties.size());
    assertNull(removed);
  }

  @Test
  public void testRemove2() throws Exception {
    Properties properties = Properties.create();
    PropertyValue removed;

    properties.set(KEY_1, BOOL_VAL_1);
    removed = properties.remove(Property.create(KEY_1, BOOL_VAL_1));
    assertEquals(0, properties.size());
    assertNotNull(removed);

    properties.set(KEY_1, BOOL_VAL_1);
    removed = properties.remove(Property.create(KEY_2, BOOL_VAL_1));
    assertEquals(1, properties.size());
    assertNull(removed);
  }

  @Test
  public void testClear() throws Exception {
    Properties properties = Properties.create();
    properties.set(KEY_1, BOOL_VAL_1);
    properties.clear();
    assertEquals("wrong size", 0, properties.size());
  }

  @Test
  public void testSize() throws Exception {
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
  public void testIsEmpty() throws Exception {
    Properties properties = Properties.create();
    assertTrue("properties was not empty", properties.isEmpty());
    properties.set(KEY_1, BOOL_VAL_1);
    assertFalse("properties was empty", properties.isEmpty());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    Properties properties1 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties properties2 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    Properties properties3 = Properties.createFromMap(SUPPORTED_PROPERTIES);
    // override property
    properties3.set(KEY_1, INT_VAL_2);

    assertTrue("properties were not equal", properties1.equals(properties2));
    assertFalse("properties were equal", properties1.equals(properties3));

    assertTrue("different hash code", properties1.hashCode() == properties2.hashCode());
    assertTrue("same hash code", properties1.hashCode() != properties3.hashCode());

    properties1 = Properties.create();
    properties1.set(KEY_1, BOOL_VAL_1);
    properties1.set(KEY_2, INT_VAL_2);

    properties2 = Properties.create();
    properties2.set(KEY_1, BOOL_VAL_1);

    assertFalse("properties were equal", properties1.equals(properties2));
    assertTrue("same hash code", properties1.hashCode() != properties2.hashCode());
  }

  @Test
  public void testIterator() throws Exception {
    Properties properties = Properties.createFromMap(SUPPORTED_PROPERTIES);

    for (Property property : properties) {
      assertTrue(SUPPORTED_PROPERTIES.containsKey(property.getKey()));
      assertEquals(SUPPORTED_PROPERTIES.get(property.getKey()),
        property.getValue().getObject());
    }
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    Properties propertiesIn = Properties.createFromMap(SUPPORTED_PROPERTIES);

    Properties propertiesOut = writeAndReadFields(Properties.class, propertiesIn);

    assertEquals(propertiesIn, propertiesOut);
  }
}