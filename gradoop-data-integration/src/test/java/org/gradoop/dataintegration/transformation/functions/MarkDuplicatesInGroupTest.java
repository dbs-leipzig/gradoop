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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link MarkDuplicatesInGroup} function.
 */
public class MarkDuplicatesInGroupTest extends GradoopFlinkTestBase {

  /**
   * Test the reduce functionality.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testReduce() throws Exception {
    VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    List<EPGMVertex> testVertices = IntStream.range(0, 10)
      .mapToObj(i -> vertexFactory.createVertex()).collect(Collectors.toList());
    for (EPGMVertex testVertex : testVertices) {
      testVertex.setLabel("Test");
      testVertex.setProperty("a", PropertyValue.NULL_VALUE);
    }
    List<EPGMVertex> reduced = getExecutionEnvironment().fromCollection(testVertices)
      .groupBy(new GetPropertiesAsList<>(Collections.singletonList("a")))
      .reduceGroup(new MarkDuplicatesInGroup<>())
      .collect();
    assertEquals(testVertices.size(), reduced.size());
    int numberOfMarkedElements = 0;
    GradoopId duplicateId = null;
    for (EPGMVertex vertex : reduced) {
      if (vertex.hasProperty(MarkDuplicatesInGroup.PROPERTY_KEY)) {
        numberOfMarkedElements++;
      } else {
        assertNull("Duplicate ID was already found", duplicateId);
        duplicateId = vertex.getId();
      }
    }
    assertEquals(testVertices.size() - 1, numberOfMarkedElements);
    for (EPGMVertex vertex : reduced) {
      if (vertex.hasProperty(MarkDuplicatesInGroup.PROPERTY_KEY)) {
        PropertyValue propertyValue = vertex.getPropertyValue(MarkDuplicatesInGroup.PROPERTY_KEY);
        assertTrue(propertyValue.isGradoopId());
        assertEquals(duplicateId, propertyValue.getGradoopId());
      }
    }
  }
}
