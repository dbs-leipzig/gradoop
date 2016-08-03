/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
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
