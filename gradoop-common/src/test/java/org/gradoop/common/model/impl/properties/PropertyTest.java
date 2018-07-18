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

import org.junit.Test;

import static org.gradoop.common.GradoopTestUtils.writeAndReadFields;
import static org.junit.Assert.*;

public class PropertyTest {

  @Test
  public void testGetKey() throws Exception {
    Property property = new Property("key", PropertyValue.create(10));
    assertEquals("key", property.getKey());
  }

  @Test
  public void testSetKey() throws Exception {
    Property property = new Property("key", PropertyValue.create(10));
    property.setKey("newKey");
    assertEquals("newKey", property.getKey());
  }

  @Test(expected = NullPointerException.class)
  public void testSetKeyNull() {
    new Property(null, PropertyValue.create(10));
  }

  @Test(expected = NullPointerException.class)
  public void testSetKeyNull2() {
    Property property = new Property("key", PropertyValue.create(10));
    property.setKey(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetKeyEmpty() {
    new Property("", PropertyValue.create(10));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetKeyEmpty2() {
    Property property = new Property("key", PropertyValue.create(10));
    property.setKey("");
  }

  @Test
  public void testGetValue() throws Exception {
    PropertyValue propertyValue = PropertyValue.create(10);
    Property p = new Property("key", propertyValue);
    assertEquals(propertyValue, p.getValue());
  }

  @Test
  public void testSetValue() throws Exception {
    PropertyValue propertyValue = PropertyValue.create(10);
    Property p = new Property("key", PropertyValue.create(11));
    p.setValue(propertyValue);
    assertEquals(propertyValue, p.getValue());
  }

  @Test (expected = NullPointerException.class)
  public void testSetValueNull() {
    Property p = new Property("key", PropertyValue.create(11));
    p.setValue(null);
  }

  @Test (expected = NullPointerException.class)
  public void testSetValueNull2() {
    new Property("key", null);
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    Property p1 = new Property("key1", PropertyValue.create(10));
    Property p2 = new Property("key1", PropertyValue.create(10));
    Property p3 = new Property("key1", PropertyValue.create(11));
    Property p4 = new Property("key2", PropertyValue.create(10));
    Property p5 = new Property("key2", PropertyValue.create(11));

    assertEquals(p1, p1);
    assertEquals(p1, p2);
    assertNotEquals(p1, p3);
    assertNotEquals(p1, p4);
    assertNotEquals(p1, p5);

    assertTrue(p1.hashCode() == p1.hashCode());
    assertTrue(p1.hashCode() == p2.hashCode());
    assertFalse(p1.hashCode() == p3.hashCode());
    assertFalse(p1.hashCode() == p4.hashCode());
    assertFalse(p1.hashCode() == p5.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    Property p1 = new Property("key1", PropertyValue.create(10));
    Property p2 = new Property("key1", PropertyValue.create(10));
    Property p3 = new Property("key2", PropertyValue.create(10));

    assertTrue(p1.compareTo(p1) == 0);
    assertTrue(p1.compareTo(p2) == 0);
    assertTrue(p1.compareTo(p3) < 0);
    assertTrue(p3.compareTo(p1) > 0);
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    Property p1 = new Property("key", PropertyValue.create(10));

    Property p2 = writeAndReadFields(Property.class, p1);

    assertEquals(p1, p2);
  }
}