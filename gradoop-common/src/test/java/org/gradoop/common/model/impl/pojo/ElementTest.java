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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.mockito.InjectMocks;
import org.testng.annotations.Test;

import static org.gradoop.common.GradoopTestUtils.KEY_0;
import static org.gradoop.common.GradoopTestUtils.STRING_VAL_6;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

public class ElementTest {

  @InjectMocks
  private EPGMVertex vertex = mock(EPGMVertex.class, CALLS_REAL_METHODS);
  private EPGMEdge edge = mock(EPGMEdge.class, CALLS_REAL_METHODS);
  private EPGMGraphHead head = mock(EPGMGraphHead.class, CALLS_REAL_METHODS);

  @Test
  public void testSetId() {
    GradoopId id = GradoopId.get();
    vertex.setId(id);
    edge.setId(id);
    head.setId(id);

    assertSame(id, vertex.getId());
    assertSame(id, edge.getId());
    assertSame(id, head.getId());
  }

  @Test
  public void testSetProperty() {
    vertex.setProperty(KEY_0, STRING_VAL_6);
    edge.setProperty(KEY_0, STRING_VAL_6);
    head.setProperty(KEY_0, STRING_VAL_6);

    Properties properties = Properties.create();
    properties.set(KEY_0, STRING_VAL_6);

    assertEquals(vertex.getProperties(), properties);
    assertEquals(edge.getProperties(), properties);
    assertEquals(head.getProperties(), properties);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testSetPropertyNull() {
    vertex.setProperty(null);
    edge.setProperty(null);
    head.setProperty(null);
  }

//  @Test
//  public void testRemoveExistentProperty() {
//    Properties properties = Properties.create();
//    properties.set(KEY_0, STRING_VAL_6);
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock with property
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", properties)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    assertEquals(properties.get(KEY_0), elementMock.removeProperty(KEY_0));
//    assertFalse(elementMock.hasProperty(KEY_0));
//  }
//
//  @Test
//  public void testRemovePropertyNoProperties() {
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock without properties
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", null)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    assertNull(elementMock.removeProperty(KEY_1));
//  }
//
//  @Test(expectedExceptions = NullPointerException.class)
//  public void testGetPropertyValueNull() {
//    Properties properties = Properties.create();
//    properties.set(KEY_0, STRING_VAL_6);
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock with property
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", properties)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    elementMock.getPropertyValue(null);
//  }
//
//  @Test
//  public void testGetPropertyNoProperties() {
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock without properties
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", null)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    assertNull(elementMock.getPropertyValue(KEY_0));
//  }
//
//  @Test
//  public void testHasPropertyNoProperties() {
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock without properties
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", null)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    assertFalse(elementMock.hasProperty(KEY_0));
//  }
//
//  @Test
//  public void testGetPropertyKeysNoProperties() {
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock without properties
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", null)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    assertNull(elementMock.getPropertyKeys());
//  }
//
//  @Test
//  public void testGetPropertyKeys() {
//    Properties properties = Properties.create();
//    properties.set(KEY_0, STRING_VAL_6);
//    properties.set(KEY_1, INT_VAL_2);
//    properties.set(KEY_2, LONG_VAL_3);
//    GradoopId gradoopId = GradoopId.get();
//
//    // create element mock with property
//    EPGMElement elementMock = mock(EPGMElement.class, withSettings()
//      .useConstructor(gradoopId, "someLabel", properties)
//      .defaultAnswer(CALLS_REAL_METHODS));
//
//    for (String key : elementMock.getPropertyKeys()) {
//      assertTrue(key.equals(KEY_0) || key.equals(KEY_1) || key.equals(KEY_2));
//    }
//  }
}
