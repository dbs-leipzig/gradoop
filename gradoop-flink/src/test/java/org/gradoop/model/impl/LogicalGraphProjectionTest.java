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
package org.gradoop.model.impl;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.helper.UnaryFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.gradoop.GradoopTestBaseUtils.PROPERTY_KEY_SINCE;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphProjectionTest extends FlinkTestBase {

  public LogicalGraphProjectionTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void projectionTest() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      forumGraph = getGraphStore().getGraph(3L);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = forumGraph.project(new VertexLabelProjectionFunction(),
      new EdgePropertyProjectionFunction());
    Comparator<Vertex<Long, DefaultVertexData>> vertexComp =
      new Comparator<Vertex<Long, DefaultVertexData>>() {
        @Override
        public int compare(Vertex<Long, DefaultVertexData> vertex1,
          Vertex<Long, DefaultVertexData> vertex2) {
          return Long.compare(vertex1.getId(), vertex2.getId());
        }
      };
    Comparator<Edge<Long, DefaultEdgeData>> edgeComp =
      new Comparator<Edge<Long, DefaultEdgeData>>() {
        @Override
        public int compare(Edge<Long, DefaultEdgeData> edge1,
          Edge<Long, DefaultEdgeData> edge2) {
          return Long
            .compare(edge1.getValue().getId(), edge2.getValue().getId());
        }
      };
    List<Vertex<Long, DefaultVertexData>> oldVertices =
      forumGraph.getGellyGraph().getVertices().collect();
    Collections.sort(oldVertices, vertexComp);
    List<Edge<Long, DefaultEdgeData>> oldEdges =
      forumGraph.getGellyGraph().getEdges().collect();
    Collections.sort(oldEdges, edgeComp);
    List<Vertex<Long, DefaultVertexData>> newVertices =
      newGraph.getGellyGraph().getVertices().collect();
    Collections.sort(newVertices, vertexComp);
    List<Edge<Long, DefaultEdgeData>> newEdges =
      newGraph.getGellyGraph().getEdges().collect();
    Collections.sort(newEdges, edgeComp);
    assertNotNull("graph was null", newGraph);
    assertEquals(forumGraph.getVertexCount(), newGraph.getVertexCount());
    assertEquals(forumGraph.getEdgeCount(), newGraph.getEdgeCount());
    assertEquals(forumGraph.getLabel(), newGraph.getLabel());
    assertEquals(forumGraph.getProperties(), newGraph.getProperties());

    for (int i = 0; i < newVertices.size(); i++) {
      Vertex<Long, DefaultVertexData> oldVertex = oldVertices.get(i);
      Vertex<Long, DefaultVertexData> newVertex = newVertices.get(i);
      assertEquals(oldVertex.getId(), newVertex.getId());
      assertEquals(oldVertex.getValue().getProperties(),
        newVertex.getValue().getProperties());
      assertEquals(newVertex.getValue().getLabel(), "test_label");
    }
    for (int i = 0; i < newEdges.size(); i++) {
      Edge<Long, DefaultEdgeData> oldEdge = oldEdges.get(i);
      Edge<Long, DefaultEdgeData> newEdge = newEdges.get(i);
      assertEquals(oldEdge.getValue().getId(), newEdge.getValue().getId());
      assertEquals(oldEdge.getValue().getLabel(),
        newEdge.getValue().getLabel());
      assertEquals(newEdge.getValue().getProperties().get("test_property"),
        "test_value");
      assertNull(newEdge.getValue().getProperties().get(PROPERTY_KEY_SINCE));
    }
  }

  private static class VertexLabelProjectionFunction implements
    UnaryFunction<DefaultVertexData, DefaultVertexData> {
    @Override
    public DefaultVertexData execute(DefaultVertexData vertexData) throws
      Exception {
      vertexData.setLabel("test_label");
      return vertexData;
    }
  }

  private static class EdgePropertyProjectionFunction implements
    UnaryFunction<DefaultEdgeData, DefaultEdgeData> {
    @Override
    public DefaultEdgeData execute(DefaultEdgeData edgeData) throws Exception {
      edgeData.setProperty("test_property", "test_value");
      edgeData.getProperties().remove(PROPERTY_KEY_SINCE);
      return edgeData;
    }
  }
}
