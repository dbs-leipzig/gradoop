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
package org.gradoop.model.impl.operators.logicalgraph.unary;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
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
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      forumGraph = getGraphStore().getGraph(GradoopId.fromLong(3L));
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      newGraph = forumGraph.project(new VertexLabelProjectionFunction(),
      new EdgePropertyProjectionFunction());

    List<VertexPojo> oldVertices = Lists.newArrayList();
    List<EdgePojo> oldEdges = Lists.newArrayList();
    List<VertexPojo> newVertices = Lists.newArrayList();
    List<EdgePojo> newEdges = Lists.newArrayList();

    forumGraph.getVertices().output(
      new LocalCollectionOutputFormat<>(oldVertices));
    forumGraph.getEdges().output(
      new LocalCollectionOutputFormat<>(oldEdges));
    newGraph.getVertices().output(
      new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(
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
      VertexPojo oldVertex = oldVertices.get(i);
      VertexPojo newVertex = newVertices.get(i);
      assertEquals(oldVertex.getId(), newVertex.getId());
      assertEquals(oldVertex.getProperties(), newVertex.getProperties());
      assertEquals(newVertex.getLabel(), "test_label");
    }
    for (int i = 0; i < newEdges.size(); i++) {
      EdgePojo oldEdge = oldEdges.get(i);
      EdgePojo newEdge = newEdges.get(i);
      assertEquals(oldEdge.getId(), newEdge.getId());
      assertEquals(oldEdge.getLabel(), newEdge.getLabel());
      assertEquals(newEdge.getProperties().get("test_property"), "test_value");
      assertNull(newEdge.getProperties().get(PROPERTY_KEY_SINCE));
    }
  }

  private static class VertexComparator
    implements Comparator<VertexPojo> {
    @Override
    public int compare(VertexPojo vertex1,
      VertexPojo vertex2) {
      return vertex1.getId().compareTo(vertex2.getId());
    }
  }

  private static class EdgeComparator
    implements Comparator<EdgePojo> {
    @Override
    public int compare(EdgePojo edge1,
      EdgePojo edge2) {
      return edge1.getId().compareTo(edge2.getId());
    }
  }

  private static class VertexLabelProjectionFunction implements
    UnaryFunction<VertexPojo, VertexPojo> {
    @Override
    public VertexPojo execute(VertexPojo vertexData) throws
      Exception {
      vertexData.setLabel("test_label");
      return vertexData;
    }
  }

  private static class EdgePropertyProjectionFunction implements
    UnaryFunction<EdgePojo, EdgePojo> {
    @Override
    public EdgePojo execute(EdgePojo edgeData) throws Exception {
      edgeData.setProperty("test_property", "test_value");
      edgeData.getProperties().remove(PROPERTY_KEY_SINCE);
      return edgeData;
    }
  }
}
