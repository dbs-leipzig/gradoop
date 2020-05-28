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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test the {@link CreateMappingFromMarkedDuplicates} function.
 */
public class CreateMappingFromMarkedDuplicatesTest extends GradoopFlinkTestBase {

  /**
   * Test the flat map function.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFlatMapFunction() throws Exception {
    VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    GradoopId duplicateId = GradoopId.get();
    EPGMVertex vertexWithProp = vertexFactory.createVertex();
    vertexWithProp.setProperty(MarkDuplicatesInGroup.PROPERTY_KEY, PropertyValue.create(duplicateId));
    EPGMVertex vertexWithProp2 = vertexFactory.createVertex();
    vertexWithProp2.setProperty(MarkDuplicatesInGroup.PROPERTY_KEY, PropertyValue.create(duplicateId));
    EPGMVertex vertexWithoutProp = vertexFactory.createVertex();
    List<EPGMVertex> vertices = Arrays.asList(vertexWithoutProp, vertexWithProp, vertexWithProp2);
    vertices.sort(Comparator.comparing(EPGMElement::getId));
    List<Tuple2<GradoopId, GradoopId>> mapping = getExecutionEnvironment()
      .fromCollection(vertices).flatMap(new CreateMappingFromMarkedDuplicates<>())
      .collect();
    assertEquals(2, mapping.size());
    assertNotEquals(mapping.get(0).f0, mapping.get(1).f0);
    assertEquals(duplicateId, mapping.get(0).f1);
    assertEquals(duplicateId, mapping.get(1).f1);
    assertNotEquals(vertexWithoutProp.getId(), mapping.get(0).f0);
    assertNotEquals(vertexWithoutProp.getId(), mapping.get(1).f0);
  }
}
