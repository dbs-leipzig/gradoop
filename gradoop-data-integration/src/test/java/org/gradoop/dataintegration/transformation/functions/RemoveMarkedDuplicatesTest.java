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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link RemoveMarkedDuplicates} filter function.
 */
public class RemoveMarkedDuplicatesTest extends GradoopFlinkTestBase {

  /**
   * Test the filter function.
   *
   * @throws Exception when execution in Flink fails.
   */
  @Test
  public void testFilter() throws Exception {
    VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    EPGMVertex vertexWithProperty = vertexFactory.createVertex();
    vertexWithProperty.setProperty(MarkDuplicatesInGroup.PROPERTY_KEY, GradoopId.get());
    EPGMVertex vertexWithoutProperty = vertexFactory.createVertex();
    List<EPGMVertex> filtered = getExecutionEnvironment()
      .fromElements(vertexWithProperty, vertexWithoutProperty)
      .filter(new RemoveMarkedDuplicates<>()).collect();
    assertEquals(1, filtered.size());
    assertEquals(vertexWithoutProperty.getId(), filtered.get(0).getId());
  }
}
