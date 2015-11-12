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

package org.gradoop.model.impl.operators.logicalgraph.binary;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Set;

import static org.gradoop.GradoopTestBaseUtils.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphCombineTest extends BinaryGraphOperatorsTestBase {

  public LogicalGraphCombineTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSameGraph() throws Exception {
    Long firstGraphID = 0L;
    Long secondGraphID = 0L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> first =
      getGraphStore().getGraph(firstGraphID);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> second =
      getGraphStore().getGraph(secondGraphID);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> result =
      first.combine(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    Long firstGraphID = 0L;
    Long secondGraphID = 2L;
    long expectedVertexCount = 5L;
    long expectedEdgeCount = 8L;
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> first =
      getGraphStore().getGraph(firstGraphID);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> second =
      getGraphStore().getGraph(secondGraphID);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> result =
      first.combine(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingSwitchedGraphs() throws Exception {
    Long firstGraphID = 2L;
    Long secondGraphID = 0L;
    long expectedVertexCount = 5L;
    long expectedEdgeCount = 8L;
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> first =
      getGraphStore().getGraph(firstGraphID);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> second =
      getGraphStore().getGraph(secondGraphID);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> result =
      first.combine(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    Long firstGraphID = 0L;
    Long secondGraphID = 1L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 8L;
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> first =
      getGraphStore().getGraph(firstGraphID);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> second =
      getGraphStore().getGraph(secondGraphID);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> result =
      first.combine(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingSwitchedGraphs() throws Exception {
    Long firstGraphID = 1L;
    Long secondGraphID = 0L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 8L;
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> first =
      getGraphStore().getGraph(firstGraphID);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> second =
      getGraphStore().getGraph(secondGraphID);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> result =
      first.combine(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testAssignment() throws Exception {
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      databaseCommunity = getGraphStore().getGraph(0L);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      graphCommunity = getGraphStore().getGraph(1L);

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      newGraph = graphCommunity.combine(databaseCommunity);

    // use collections as data sink
    Collection<VertexPojo> vertexData = Lists.newArrayList();
    Collection<EdgePojo> edgeData = Lists.newArrayList();

    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertexData));
    newGraph.getEdges()
      .output(new LocalCollectionOutputFormat<>(edgeData));

    getExecutionEnvironment().execute();

    for (EPGMVertex v : vertexData) {
      Set<Long> gIDs = v.getGraphIds();
      if (v.equals(VERTEX_PERSON_ALICE)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_BOB)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_EVE)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_CAROL)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_DAVE)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_FRANK)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }

    for (EPGMEdge e : edgeData) {
      Set<Long> gIDs = e.getGraphIds();
      if (e.equals(EDGE_0_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(EDGE_1_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(EDGE_6_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(EDGE_21_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(EDGE_4_KNOWS)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (e.equals(EDGE_5_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(EDGE_22_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(EDGE_23_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }
  }
}
