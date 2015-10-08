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
package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
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

    List<DefaultVertexData> oldVertices = Lists.newArrayList();
    List<DefaultEdgeData> oldEdges = Lists.newArrayList();
    List<DefaultVertexData> newVertices = Lists.newArrayList();
    List<DefaultEdgeData> newEdges = Lists.newArrayList();

    forumGraph.getVertexData().output(
      new LocalCollectionOutputFormat<>(oldVertices));
    forumGraph.getEdgeData().output(
      new LocalCollectionOutputFormat<>(oldEdges));
    newGraph.getVertexData().output(
      new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdgeData().output(
      new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    Collections.sort(oldVertices, new VertexComparator());
    Collections.sort(newVertices, new VertexComparator());
    Collections.sort(oldEdges, new EdgeComparator());
    Collections.sort(newEdges, new EdgeComparator());

    assertNotNull("graph was null", newGraph);
    assertEquals(oldVertices.size(), newVertices.size());
    assertEquals(oldEdges.size(), newEdges.size());
    assertEquals(forumGraph.getLabel(), newGraph.getLabel());
    assertEquals(forumGraph.getProperties(), newGraph.getProperties());

    for (int i = 0; i < newVertices.size(); i++) {
      DefaultVertexData oldVertex = oldVertices.get(i);
      DefaultVertexData newVertex = newVertices.get(i);
      assertEquals(oldVertex.getId(), newVertex.getId());
      assertEquals(oldVertex.getProperties(), newVertex.getProperties());
      assertEquals(newVertex.getLabel(), "test_label");
    }
    for (int i = 0; i < newEdges.size(); i++) {
      DefaultEdgeData oldEdge = oldEdges.get(i);
      DefaultEdgeData newEdge = newEdges.get(i);
      assertEquals(oldEdge.getId(), newEdge.getId());
      assertEquals(oldEdge.getLabel(), newEdge.getLabel());
      assertEquals(newEdge.getProperties().get("test_property"), "test_value");
      assertNull(newEdge.getProperties().get(PROPERTY_KEY_SINCE));
    }
  }

  private static class VertexComparator
    implements Comparator<DefaultVertexData> {
    @Override
    public int compare(DefaultVertexData vertex1,
      DefaultVertexData vertex2) {
      return Long.compare(vertex1.getId(), vertex2.getId());
    }
  }

  private static class EdgeComparator
    implements Comparator<DefaultEdgeData> {
    @Override
    public int compare(DefaultEdgeData edge1,
      DefaultEdgeData edge2) {
      return Long
        .compare(edge1.getId(), edge2.getId());
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
