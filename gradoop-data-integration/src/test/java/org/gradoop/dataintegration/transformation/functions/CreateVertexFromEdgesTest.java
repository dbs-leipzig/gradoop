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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link CreateVertexFromEdges} function used by
 * {@link org.gradoop.dataintegration.transformation.EdgeToVertex}.
 */
public class CreateVertexFromEdgesTest extends GradoopFlinkTestBase {
  /**
   * Test the function by applying it to some vertices.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFunction() throws Exception {
    CreateVertexFromEdges<EPGMVertex, EPGMEdge> function = new CreateVertexFromEdges<>("test",
      getConfig().getLogicalGraphFactory().getVertexFactory());
    GradoopId dummy = GradoopId.get();
    EdgeFactory<EPGMEdge> edgeFactory = getConfig().getLogicalGraphFactory().getEdgeFactory();
    // Create some test edges, with some having no properties or label.
    EPGMEdge withoutProperties = edgeFactory.createEdge(dummy, dummy);
    EPGMEdge withProperties = edgeFactory.createEdge("TestEdge2", dummy, dummy);
    withProperties.setProperty("TestProperty", 1L);
    List<EPGMEdge> edges = Arrays.asList(withoutProperties, withProperties);
    List<Tuple3<EPGMVertex, GradoopId, GradoopId>> result = getExecutionEnvironment()
      .fromCollection(edges).map(function).collect();
    // There should be a new vertex for each edge.
    assertEquals(edges.size(), result.size());
    // Every ID should be assigned only once.
    long idCount = result.stream().map(t -> t.f0.getId()).distinct().count();
    assertEquals("EPGMVertex IDs are not unique.", edges.size(), idCount);
    for (Tuple3<EPGMVertex, GradoopId, GradoopId> resultTuple : result) {
      if (resultTuple.f0.getPropertyCount() > 0) {
        assertEquals(withProperties.getProperties(), resultTuple.f0.getProperties());
      }
    }
  }
}
