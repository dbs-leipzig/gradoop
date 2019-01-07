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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Test for the {@link CreateEdgesFromTriple} function used by
 * {@link org.gradoop.dataintegration.transformation.EdgeToVertex}.
 */
public class CreateEdgesFromTripleTest extends GradoopFlinkTestBase {

  /**
   * Test the function by applying it to some tuples.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testFunction() throws Exception {
    CreateEdgesFromTriple<Vertex, Edge> function = new CreateEdgesFromTriple<>(
      getConfig().getEdgeFactory(), "source", "target");
    VertexFactory vertexFactory = getConfig().getVertexFactory();
    Vertex testVertex1 = vertexFactory.createVertex();
    Vertex testVertex2 = vertexFactory.createVertex();
    GradoopId source1 = GradoopId.get();
    GradoopId source2 = GradoopId.get();
    GradoopId target1 = GradoopId.get();
    GradoopId target2 = GradoopId.get();
    Tuple3<Vertex, GradoopId, GradoopId> tuple1 = new Tuple3<>(testVertex1, source1, target1);
    Tuple3<Vertex, GradoopId, GradoopId> tuple2 = new Tuple3<>(testVertex2, source2, target2);
    List<Edge> result = getExecutionEnvironment().fromElements(tuple1, tuple2).flatMap(function)
      .collect();
    // Check if the correct number of edges were created and if they are distinct.
    assertEquals(4, result.size());
    // By id.
    assertEquals(4, result.stream().map(Element::getId).count());
    // By source and target id.
    assertEquals(4,
      result.stream().map(e -> Tuple2.of(e.getSourceId(), e.getTargetId())).distinct().count());
    // Finally check the data of the edges.
    for (Edge resultEdge : result) {
      if (resultEdge.getLabel().equals("source")) {
        if (resultEdge.getSourceId().equals(source1)) {
          assertEquals(testVertex1.getId(), resultEdge.getTargetId());
        } else if (resultEdge.getSourceId().equals(source2)) {
          assertEquals(testVertex2.getId(), resultEdge.getTargetId());
        } else {
          fail("Edge with invalid source ID created.");
        }
      } else if (resultEdge.getLabel().equals("target")) {
        if (resultEdge.getSourceId().equals(testVertex1.getId())) {
          assertEquals(target1, resultEdge.getTargetId());
        } else if (resultEdge.getSourceId().equals(testVertex2.getId())) {
          assertEquals(target2, resultEdge.getTargetId());
        } else {
          fail("Edge with invalid source ID created.");
        }
      } else {
        fail("Edge with invalid label created.");
      }
    }
  }
}
