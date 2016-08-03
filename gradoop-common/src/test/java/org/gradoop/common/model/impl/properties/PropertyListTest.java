package org.gradoop.common.model.impl.properties;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
public class PropertyListTest {

  @Test
  public void testCreateFromMap() throws Exception {
    PropertyList properties = PropertyList.createFromMap(SUPPORTED_PROPERTIES);

    assertEquals(SUPPORTED_PROPERTIES.size(), properties.size());

    for (Map.Entry<String, Object> entry : SUPPORTED_PROPERTIES.entrySet()) {
      String k = entry.getKey();
      assertTrue(properties.containsKey(k));
      assertEquals(entry.getValue(), properties.get(k).getObject());
    }
  }

  @Test
  public void testGetKeys() throws Exception {
    PropertyList properties = PropertyList.createFromMap(SUPPORTED_PROPERTIES);

    List<String> keyList = Lists.newArrayList(properties.getKeys());

    assertEquals(SUPPORTED_PROPERTIES.size(), keyList.size());
    for (String expectedKey : SUPPORTED_PROPERTIES.keySet()) {
      assertTrue("key was not in key list", keyList.contains(expectedKey));
    }
  }

  @Test
  public void testContainsKey() throws Exception {
    PropertyList properties = PropertyList.create();

    assertFalse("unexpected key found", properties.containsKey(KEY_1));
    properties.set(KEY_1, BOOL_VAL_1);
    assertTrue("key not found", properties.containsKey(KEY_1));
    assertFalse("unexpected key found", properties.containsKey("1234"));
  }

  @Test
  public void testGet() throws Exception {
    PropertyList properties = PropertyList.createFromMap(SUPPORTED_PROPERTIES);

    assertNotNull("property was null", properties.get(KEY_1));
    assertEquals("wrong property", BOOL_VAL_1, properties.get(KEY_1).getBoolean());
    assertNull("unexpected property", properties.get("1234"));
  }

  @Test
  public void testSet() throws Exception {
    PropertyList properties = PropertyList.create();

    properties.set(Property.create(KEY_1, BOOL_VAL_1));
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(Property.create(KEY_1, INT_VAL_2));
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testSet1() throws Exception {
    PropertyList properties = PropertyList.create();

    properties.set(KEY_1, PropertyValue.create(BOOL_VAL_1));
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(KEY_1, PropertyValue.create(INT_VAL_2));
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testSet2() throws Exception {
    PropertyList properties = PropertyList.create();

    properties.set(KEY_1, BOOL_VAL_1);
    assertEquals(BOOL_VAL_1, properties.get(KEY_1).getObject());

    // override
    properties.set(KEY_1, INT_VAL_2);
    assertEquals(INT_VAL_2, properties.get(KEY_1).getObject());
  }

  @Test
  public void testSize() throws Exception {
    PropertyList properties = PropertyList.create();
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
    PropertyList properties = PropertyList.create();
    assertTrue("properties was not empty", properties.isEmpty());
    properties.set(KEY_1, BOOL_VAL_1);
    assertFalse("properties was empty", properties.isEmpty());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    PropertyList properties1 = PropertyList.createFromMap(SUPPORTED_PROPERTIES);
    PropertyList properties2 = PropertyList.createFromMap(SUPPORTED_PROPERTIES);
    PropertyList properties3 = PropertyList.createFromMap(SUPPORTED_PROPERTIES);
    // override property
    properties3.set(KEY_1, INT_VAL_2);

    assertTrue("properties were not equal", properties1.equals(properties2));
    assertFalse("properties were equal", properties1.equals(properties3));

    assertTrue("different hash code",
      properties1.hashCode() == properties2.hashCode());
    assertTrue("same hash code",
      properties1.hashCode() != properties3.hashCode());

    properties1 = PropertyList.create();
    properties1.set(KEY_1, BOOL_VAL_1);
    properties1.set(KEY_2, INT_VAL_2);

    properties2 = PropertyList.create();
    properties2.set(KEY_2, INT_VAL_2);
    properties2.set(KEY_1, BOOL_VAL_1);

    assertFalse("properties were equal", properties1.equals(properties2));
    assertTrue("same hash code",
      properties1.hashCode() != properties2.hashCode());
  }

  @Test
  public void testIterator() throws Exception {
    PropertyList properties = PropertyList.createFromMap(SUPPORTED_PROPERTIES);

    for (Property property : properties) {
      assertTrue(SUPPORTED_PROPERTIES.containsKey(property.getKey()));
      assertEquals(SUPPORTED_PROPERTIES.get(property.getKey()),
        property.getValue().getObject());
    }
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    PropertyList propertiesIn = PropertyList.createFromMap(SUPPORTED_PROPERTIES);

    PropertyList propertiesOut = writeAndReadFields(PropertyList.class, propertiesIn);

    assertEquals(propertiesIn, propertiesOut);
  }
}