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

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
      newGraph = dbGraph.sampleRandomNodes(3l);
    List<Vertex<Long, DefaultVertexData>> dbVertices =
      dbGraph.getVertices().collect();
    List<Vertex<Long, DefaultVertexData>> newVertices =
      newGraph.getVertices().collect();
    List<Edge<Long, DefaultEdgeData>> dbEdges = dbGraph.getEdges().collect();
    List<Edge<Long, DefaultEdgeData>> newEdges = dbGraph.getEdges().collect();
    assertNotNull("graph was null", newGraph);
    System.out.println(newVertices.size());
    assertTrue(newVertices.size() > 0);
    for (Vertex<Long, DefaultVertexData> vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
    }
    for (Edge<Long, DefaultEdgeData> edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
    }
  }
}
