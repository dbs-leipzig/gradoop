package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.Test;
import org.mockito.Mockito;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class ElementTest {

  @Test
  public void testSetId() {
    Element elementMock = mock(Element.class, Mockito.CALLS_REAL_METHODS);
    GradoopId id = new GradoopId();

    elementMock.setId(id);

    assertSame(id, elementMock.id);
  }

  @Test
  public void testSetProperty() {
    Element elementMock = mock(Element.class, Mockito.CALLS_REAL_METHODS);

    elementMock.setProperty("key", "");

    Properties properties = new Properties();
    properties.set("key", "");

    assertEquals(elementMock.properties, properties);
  }

  @Test
  public void testSetPropertyNull() {
    Element elementMock = mock(Element.class, Mockito.CALLS_REAL_METHODS);

    try {
      elementMock.setProperty(null);
      fail("Expected an NullPointerException to be thrown");
    } catch (NullPointerException nullPointerException) {
      assertSame(nullPointerException.getMessage(), "Property was null");
    }
  }

  @Test
  public void testRemoveExistentProperty() {
    Properties properties = new Properties();
    properties.set("someKey", "someValue");
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertEquals(properties.get("someKey"), elementMock.removeProperty("someKey"));
    assertFalse(elementMock.hasProperty("someKey"));
  }

  @Test
  public void testRemovePropertyNoProperties() {
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.removeProperty("otherKey"));
  }

  @Test(expected = NullPointerException.class)
  public void testGetPropertyValueNull() {
    Properties properties = new Properties();
    properties.set("someKey", "someValue");
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.getPropertyValue(null);
  }

  @Test
  public void testGetPropertyNoProperties() {
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyValue("someKey"));
  }

  @Test
  public void testHasPropertyNoProperties() {
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertFalse(elementMock.hasProperty("someKey"));
  }

  @Test
  public void testGetPropertyKeysNoProperties() {
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock without properties
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertNull(elementMock.getPropertyKeys());
  }

  @Test
  public void testGetPropertyKeys() {
    Properties properties = new Properties();
    properties.set("key1", "someValue1");
    properties.set("key2", "someValue2");
    properties.set("key3", "someValue3");
    GradoopId gradoopIdMock = mock(GradoopId.class);

    // create element mock with property
    Element elementMock = mock(Element.class, withSettings()
      .useConstructor(gradoopIdMock, "someLabel", properties)
      .defaultAnswer(CALLS_REAL_METHODS));

    for(String key : elementMock.getPropertyKeys()) {
      assertTrue(key.equals("key1") || key.equals("key2") || key.equals("key3"));
    }
  }
}
