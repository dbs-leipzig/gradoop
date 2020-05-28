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
package org.gradoop.temporal.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Tests of class {@link TemporalGraphElement}.
 */
public class TemporalGraphElementTest {

  /**
   * Test {@link TemporalGraphElement#getGraphIds()}.
   */
  @Test
  public void testGetGraphIds() {
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, graphIds, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertEquals(graphIds, elementMock.getGraphIds());
  }

  /**
   * Test {@link TemporalGraphElement#addGraphId(GradoopId)}.
   */
  @Test
  public void testAddGraphId() {
    GradoopId additionalId = GradoopId.get();
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, graphIds, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.addGraphId(additionalId);
    graphIds.add(additionalId);
    assertEquals(graphIds, elementMock.getGraphIds());
  }

  /**
   * Test {@link TemporalGraphElement#addGraphId(GradoopId)} if no ids were set before.
   */
  @Test
  public void testAddGraphIdWithEmptyIds() {
    GradoopId additionalId = GradoopId.get();

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.addGraphId(additionalId);
    assertEquals(GradoopIdSet.fromExisting(additionalId), elementMock.getGraphIds());
  }

  /**
   * Test {@link TemporalGraphElement#setGraphIds(GradoopIdSet)}
   */
  @Test
  public void testSetGraphIds() {
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, null, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.setGraphIds(graphIds);
    assertEquals(graphIds, elementMock.getGraphIds());
  }

  /**
   * Test {@link TemporalGraphElement#resetGraphIds()}.
   */
  @Test
  public void testResetGraphIds() {
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, graphIds, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    elementMock.resetGraphIds();

    assertTrue(elementMock.getGraphIds().isEmpty());
  }

  /**
   * Test {@link TemporalGraphElement#getGraphCount()}.
   */
  @Test
  public void testGetGraphCount() {
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(GradoopId.get(), GradoopId.get());

    TemporalGraphElement elementMock = mock(TemporalGraphElement.class, withSettings()
      .useConstructor(GradoopId.get(), "x", null, graphIds, null, null)
      .defaultAnswer(CALLS_REAL_METHODS));

    assertEquals(2, elementMock.getGraphCount());
  }
}
