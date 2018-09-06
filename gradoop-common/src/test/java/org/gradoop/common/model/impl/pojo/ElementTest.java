package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class ElementTest {

  @Test
  public void testSetId() {
    Element elementMock = mock(Element.class, CALLS_REAL_METHODS);
    GradoopId id = GradoopId.get();
    elementMock.setId(id);

    assertSame(id, elementMock.getId());
  }

  @Test
  public void testSetProperty() {
    Element elementMock = mock(Element.class, CALLS_REAL_METHODS);
    elementMock.setProperty("key", "");

    Properties properties = Properties.create();
    properties.set("key", "");

    assertEquals(elementMock.getProperties(), properties);
  }

  @Test(expected = NullPointerException.class)
  public void testSetPropertyNull() {
    Element elementMock = mock(Element.class, CALLS_REAL_METHODS);
    elementMock.setProperty(null);
  }

  @Test
  public void testRemoveExistentProperty() {
    Properties properties = Properties.create();
    properties.set("someKey", "someValue");
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertEquals(properties.get("someKey"), elementMock.removeProperty("someKey"));
    assertFalse(elementMock.hasProperty("someKey"));
  }

  @Test
  public void testRemovePropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.removeProperty("otherKey"));
  }

  @Test(expected = NullPointerException.class)
  public void testGetPropertyValueNull() {
    Properties properties = Properties.create();
    properties.set("someKey", "someValue");
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.getPropertyValue(null);
  }

  @Test
  public void testGetPropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyValue("someKey"));
  }

  @Test
  public void testHasPropertyNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertFalse(elementMock.hasProperty("someKey"));
  }

  @Test
  public void testGetPropertyKeysNoProperties() {
    GradoopId gradoopId = GradoopId.get();

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyKeys());
  }

  @Test
  public void testGetPropertyKeys() {
    Properties properties = Properties.create();
    properties.set("key1", "someValue1");
    properties.set("key2", "someValue2");
    properties.set("key3", "someValue3");
    GradoopId gradoopId = GradoopId.get();

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopId, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    for(String key : elementMock.getPropertyKeys()) {
      assertTrue(key.equals("key1") || key.equals("key2") || key.equals("key3"));
    }
  }
}
