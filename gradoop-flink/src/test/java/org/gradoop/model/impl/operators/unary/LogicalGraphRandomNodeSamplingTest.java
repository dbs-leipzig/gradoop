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
package org.gradoop.model.impl.operators.unary;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphRandomNodeSamplingTest extends FlinkTestBase {
  public LogicalGraphRandomNodeSamplingTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void randomNodeSamplingTest() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> dbGraph =
      getGraphStore().getDatabaseGraph();
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = dbGraph.sampleRandomNodes(0.272f);
    List<Vertex<Long, DefaultVertexData>> dbVertices = Lists.newArrayList();
    List<Edge<Long, DefaultEdgeData>> dbEdges = Lists.newArrayList();
    List<Vertex<Long, DefaultVertexData>> newVertices = Lists.newArrayList();
    List<Edge<Long, DefaultEdgeData>> newEdges = Lists.newArrayList();
    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));
    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));
    getExecutionEnvironment().execute();
    assertNotNull("graph was null", newGraph);
    Set<Long> newVertexIDs = new HashSet<>();
    for (Vertex<Long, DefaultVertexData> vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (Edge<Long, DefaultEdgeData> edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSource()));
      assertTrue(newVertexIDs.contains(edge.getTarget()));
    }
    dbEdges.removeAll(newEdges);
    for (Edge<Long, DefaultEdgeData> edge : dbEdges) {
      assertFalse(newVertexIDs.contains(edge.getSource()) &&
        newVertexIDs.contains(edge.getTarget()));
    }
  }

  @Test
  public void randomNodeSamplingTestWithSeed() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> dbGraph =
      getGraphStore().getDatabaseGraph();
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = dbGraph.callForGraph(
      new RandomNodeSampling<DefaultVertexData, DefaultEdgeData,
        DefaultGraphData>(
        0.272f, -4181668494294894490L));
    List<Vertex<Long, DefaultVertexData>> dbVertices = Lists.newArrayList();
    List<Edge<Long, DefaultEdgeData>> dbEdges = Lists.newArrayList();
    List<Vertex<Long, DefaultVertexData>> newVertices = Lists.newArrayList();
    List<Edge<Long, DefaultEdgeData>> newEdges = Lists.newArrayList();
    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));
    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));
    getExecutionEnvironment().execute();
    assertNotNull("graph was null", newGraph);
    Set<Long> newVertexIDs = new HashSet<>();
    for (Vertex<Long, DefaultVertexData> vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    System.out.println(newVertexIDs);
    for (Edge<Long, DefaultEdgeData> edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSource()));
      assertTrue(newVertexIDs.contains(edge.getTarget()));
      System.out.println(edge.getSource() + " " + edge.getTarget());
    }
    dbEdges.removeAll(newEdges);
    for (Edge<Long, DefaultEdgeData> edge : dbEdges) {
      assertFalse(newVertexIDs.contains(edge.getSource()) &&
        newVertexIDs.contains(edge.getTarget()));
    }
  }
}
