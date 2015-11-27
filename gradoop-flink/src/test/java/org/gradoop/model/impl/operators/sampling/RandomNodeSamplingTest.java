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
package org.gradoop.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.model.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.sampling.RandomNodeSampling;
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
public class RandomNodeSamplingTest extends GradoopFlinkTestBase {
  public RandomNodeSamplingTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void randomNodeSamplingTest() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dbGraph =
      getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraph = dbGraph.sampleRandomNodes(0.272f);

    validateResult(dbGraph, newGraph);
  }

  @Test
  public void randomNodeSamplingTestWithSeed() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dbGraph =
      getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraph = dbGraph.callForGraph(
      new RandomNodeSampling<GraphHeadPojo, VertexPojo, EdgePojo>(
        0.272f, -4181668494294894490L));

    validateResult(dbGraph, newGraph);
  }

  private void validateResult(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> input,
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> output)
    throws Exception {
    List<VertexPojo> dbVertices = Lists.newArrayList();
    List<EdgePojo> dbEdges = Lists.newArrayList();
    List<VertexPojo> newVertices = Lists.newArrayList();
    List<EdgePojo> newEdges = Lists.newArrayList();

    input.getVertices()
      .output(new LocalCollectionOutputFormat<>(dbVertices));
    input.getEdges()
      .output(new LocalCollectionOutputFormat<>(dbEdges));

    output.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    output.getEdges()
      .output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", output);

    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (VertexPojo vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (EdgePojo edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSourceId()));
      assertTrue(newVertexIDs.contains(edge.getTargetId()));
    }
    dbEdges.removeAll(newEdges);
    for (EdgePojo edge : dbEdges) {
      assertFalse(
        newVertexIDs.contains(edge.getSourceId()) &&
        newVertexIDs.contains(edge.getTargetId()));
    }
  }
}
