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
package org.gradoop.temporal.model.impl.operators.verify;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test for the {@link VerifyTemporal} operator.
 */
public class VerifyTemporalTest extends TemporalGradoopTestBase {

  /**
   * Test the operator on a graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testVerify() throws Exception {
    final TemporalVertexFactory vf = getVertexFactory();
    final TemporalEdgeFactory ef = getEdgeFactory();
    final long validFromMax = asMillis("2019.03.01 12:00:00.000");
    final long validToMin = asMillis("2019.06.01 12:00:00.000");
    // Some vertices.
    TemporalVertex v1 = vf.createVertex();
    v1.setValidFrom(asMillis("2019.01.01 12:00:00.000"));
    v1.setValidTo(validToMin);
    TemporalVertex v2 = vf.createVertex();
    v2.setValidFrom(validFromMax);
    v2.setValidTo(asMillis("2019.09.01 12:00:00.000"));
    // Some edges.
    TemporalEdge e1 = ef.createEdge(v1.getId(), v2.getId());
    TemporalEdge e2 = ef.createEdge(v1.getId(), v1.getId());
    e2.setValidFrom(asMillis("2019.04.01 12:00:00.000"));
    e2.setValidTo(asMillis("2019.10.01 12:00:00.000"));
    TemporalEdge e3 = ef.createEdge(v2.getId(), v2.getId());
    e3.setValidFrom(asMillis("2018.01.01 12:00:00.000"));
    e3.setValidTo(asMillis("2019.05.01 12:00:00.000"));
    // Some dangling edges.
    TemporalEdge ed1 = ef.createEdge(v1.getId(), GradoopId.get());
    TemporalEdge ed2 = ef.createEdge(GradoopId.get(), v2.getId());
    TemporalEdge ed3 = ef.createEdge(GradoopId.get(), GradoopId.get());

    // Create graph and verify.
    DataSet<TemporalVertex> vertexSet = getExecutionEnvironment().fromElements(v1, v2);
    DataSet<TemporalEdge> edgeSet = getExecutionEnvironment().fromElements(e1, e2, e3, ed1, ed2, ed3);
    TemporalGraph graph = getConfig().getTemporalGraphFactory().fromDataSets(vertexSet, edgeSet);
    TemporalGraph result = graph.callForGraph(new VerifyTemporal());

    // Get result.
    List<TemporalVertex> resultVertices = new ArrayList<>();
    List<TemporalEdge> resultEdges = new ArrayList<>();
    result.getVertices().output(new LocalCollectionOutputFormat<>(resultVertices));
    result.getEdges().output(new LocalCollectionOutputFormat<>(resultEdges));
    getExecutionEnvironment().execute();

    assertEquals(resultVertices.size(), 2);
    assertEquals(resultEdges.size(), 3);
    // Check if correct edges were removed.
    assertEquals(resultEdges.stream().map(TemporalEdge::getId).sorted().toArray(),
      Stream.of(e1, e2, e3).map(TemporalEdge::getId).sorted().toArray());
    // Check if times were updated correctly.
    for (TemporalEdge resultEdge : resultEdges) {
      if (resultEdge.getId().equals(e1.getId())) {
        assertEquals((long) resultEdge.getValidFrom(), validFromMax);
        assertEquals((long) resultEdge.getValidTo(), validToMin);
      } else if (resultEdge.getId().equals(e2.getId())) {
        assertEquals(resultEdge.getValidFrom(), e2.getValidFrom());
        assertEquals((long) resultEdge.getValidTo(), validToMin);
      } else if (resultEdge.getId().equals(e3.getId())) {
        assertEquals((long) resultEdge.getValidFrom(), validFromMax);
        assertEquals(resultEdge.getValidTo(), e3.getValidTo());
      } else {
        fail();
      }
    }
  }
}
