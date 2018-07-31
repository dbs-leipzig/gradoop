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

public class RandomEdgeSamplingTest extends GradoopFlinkTestBase {

  @Test
  public void randomEdgeSamplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
            .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = new RandomEdgeSampling(0.272f).execute(dbGraph);

    validateResult(dbGraph, newGraph);
  }

  @Test
  public void randomEdgeSamplingTestWithSeed() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
            .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph =
            new RandomEdgeSampling(0.272f, -4181668494294894490L)
            .execute(dbGraph);

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

    // Test, if there is a result graph
    assertNotNull("graph was null", output);

    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (Vertex vertex : newVertices) {
      // Test, if all new vertices are taken from the original graph
      assertTrue("sampled vertex is not part of the original graph", dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }

    Set<GradoopId> connectedVerticesIDs = new HashSet<>();
    for (Edge edge : newEdges) {
      // Test, if all new edges are taken from the original graph
      assertTrue("sampled edge is not part of the original graph", dbEdges.contains(edge));
      // Test, if all source- and target-vertices from new edges are part of the sampled graph, too
      assertTrue("sampled edge has source vertex which is not part of the sampled graph",
              newVertexIDs.contains(edge.getSourceId()));
      connectedVerticesIDs.add(edge.getSourceId());
      assertTrue("sampled edge has target vertex which is not part of the sampled graph",
              newVertexIDs.contains(edge.getTargetId()));
      connectedVerticesIDs.add(edge.getTargetId());
    }

    // Test, if there aren't any unconnected vertices left
    newVertexIDs.removeAll(connectedVerticesIDs);
    assertTrue("there are unconnected vertices in the sampled graph", newVertexIDs.isEmpty());
  }
}