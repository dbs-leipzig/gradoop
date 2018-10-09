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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class GraphElementTest {

  @Test
  public void testAddGraphIdNoGraphIds() {
    GraphElement graphElementMock = mock(GraphElement.class, CALLS_REAL_METHODS);

    GradoopId id = GradoopId.get();
    graphElementMock.addGraphId(id);

    assertNotNull(graphElementMock.getGraphIds());
  }

  @Test
  public void testResetGraphIds() {
    Properties propertiesMock = mock(Properties.class);
    GradoopIdSet idSet = new GradoopIdSet();
    idSet.add(GradoopId.get());

    GraphElement graphElementMock = mock(GraphElement.class, withSettings()
    .useConstructor(GradoopId.get(), "someLabel", propertiesMock, idSet)
    .defaultAnswer(CALLS_REAL_METHODS));

    graphElementMock.resetGraphIds();

    assertTrue(graphElementMock.getGraphIds().isEmpty());
  }
}
