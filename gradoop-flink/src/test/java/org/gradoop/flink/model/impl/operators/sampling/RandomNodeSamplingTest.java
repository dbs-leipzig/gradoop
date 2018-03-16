/**
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
package org.gradoop.flink.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class RandomNodeSamplingTest extends GradoopFlinkTestBase {

  @Test
  public void randomNodeSamplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
      .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = dbGraph.sampleRandomNodes(0.272f);

    validateResult(dbGraph, newGraph);
  }

  @Test
  public void randomNodeSamplingTestWithSeed() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
      .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = dbGraph.callForGraph(
      new RandomNodeSampling(0.272f, -4181668494294894490L));

    validateResult(dbGraph, newGraph);
  }

  private void validateResult(LogicalGraph input, LogicalGraph output)
    throws Exception {
    List<Vertex> dbVertices = Lists.newArrayList();
    List<Edge> dbEdges = Lists.newArrayList();
    List<Vertex> newVertices = Lists.newArrayList();
    List<Edge> newEdges = Lists.newArrayList();

    input.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    input.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));

    output.getVertices().output(new LocalCollectionOutputFormat<>(newVertices));
    output.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", output);

    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (Vertex vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (Edge edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSourceId()));
      assertTrue(newVertexIDs.contains(edge.getTargetId()));
    }
    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse(
        newVertexIDs.contains(edge.getSourceId()) &&
        newVertexIDs.contains(edge.getTargetId()));
    }
  }
}
