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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.functions.epgm.Label;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link CreateMappingToFirstElementOfGroup} function.
 */
public class CreateMappingToFirstElementOfGroupTest extends GradoopFlinkTestBase {

  /**
   * Test the grouping function.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testInGrouping() throws Exception {
    VertexFactory<EPGMVertex> vf = getConfig().getLogicalGraphFactory().getVertexFactory();
    EPGMVertex a1 = vf.createVertex("a");
    EPGMVertex a2 = vf.createVertex("a");
    EPGMVertex a3 = vf.createVertex("a");
    EPGMVertex b1 = vf.createVertex("b");
    EPGMVertex b2 = vf.createVertex("b");
    EPGMVertex b3 = vf.createVertex("b");
    Map<GradoopId, EPGMVertex> idToVertex = Stream.of(a1, a2, a3, b1, b2, b3)
      .collect(Collectors.toMap(EPGMVertex::getId, i -> i));
    List<Tuple2<GradoopId, GradoopId>> mapping = getExecutionEnvironment()
      .fromElements(a1, a2, a3, b1, b2, b3)
      .groupBy(new Label<>())
      .reduceGroup(new CreateMappingToFirstElementOfGroup<>()).collect();
    assertEquals("Incorrect number of items", 6, mapping.size());
    assertEquals("Incorrect number of total vertices", 6,
      mapping.stream().map(t -> t.f0).distinct().count());
    assertEquals("Incorrect number of groups", 2,
      mapping.stream().map(t -> t.f1).distinct().count());

    for (Tuple2<GradoopId, GradoopId> entry : mapping) {
      assertEquals(idToVertex.get(entry.f1).getLabel(), idToVertex.get(entry.f0).getLabel());
    }
  }
}
