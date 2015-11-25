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
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.logicalgraph.unary.sampling.RandomNodeSampling;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphRandomNodeSamplingTest extends FlinkTestBase {
  public LogicalGraphRandomNodeSamplingTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void randomNodeSamplingTest() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dbGraph =
      getGraphStore().getDatabaseGraph();
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraph = dbGraph.sampleRandomNodes(0.272f);
    List<VertexPojo> dbVertices = Lists.newArrayList();
    List<EdgePojo> dbEdges = Lists.newArrayList();
    List<VertexPojo> newVertices = Lists.newArrayList();
    List<EdgePojo> newEdges = Lists.newArrayList();
    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));
    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));
    getExecutionEnvironment().execute();
    assertNotNull("graph was null", newGraph);
    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (VertexPojo vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (EdgePojo edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSourceVertexId()));
      assertTrue(newVertexIDs.contains(edge.getTargetVertexId()));
    }
    dbEdges.removeAll(newEdges);
    for (EdgePojo edge : dbEdges) {
      assertFalse(newVertexIDs.contains(edge.getSourceVertexId()) &&
        newVertexIDs.contains(edge.getTargetVertexId()));
    }
  }

  @Test
  public void randomNodeSamplingTestWithSeed() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dbGraph =
      getGraphStore().getDatabaseGraph();
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraph = dbGraph.callForGraph(
      new RandomNodeSampling<VertexPojo, EdgePojo, GraphHeadPojo>(
        0.272f, -4181668494294894490L));
    List<VertexPojo> dbVertices = Lists.newArrayList();
    List<EdgePojo> dbEdges = Lists.newArrayList();
    List<VertexPojo> newVertices = Lists.newArrayList();
    List<EdgePojo> newEdges = Lists.newArrayList();
    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));
    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));
    getExecutionEnvironment().execute();
    assertNotNull("graph was null", newGraph);
    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (VertexPojo vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (EdgePojo edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSourceVertexId()));
      assertTrue(newVertexIDs.contains(edge.getTargetVertexId()));
    }
    dbEdges.removeAll(newEdges);
    for (EdgePojo edge : dbEdges) {
      assertFalse(newVertexIDs.contains(edge.getSourceVertexId()) &&
        newVertexIDs.contains(edge.getTargetVertexId()));
    }
  }
}
