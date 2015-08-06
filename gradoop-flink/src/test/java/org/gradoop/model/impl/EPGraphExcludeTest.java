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

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.gradoop.model.EdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.VertexData;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class EPGraphExcludeTest extends EPFlinkTest {

  private EPGraphStore graphStore;

  public EPGraphExcludeTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0, 0, 0, 0", // same graph
    "0, 2, 1, 0", // overlapping graphs
    "2, 0, 2, 2", // overlapping graphs switched (different counts)
    "0, 1, 3, 4", // non-overlapping graphs
    "1, 0, 3, 4", // non-overlapping graphs switched
  })
  public void testExclude(long firstGraph, long secondGraph,
    long expectedVertexCount, long expectedEdgeCount) throws Exception {
    EPGraph first = graphStore.getGraph(firstGraph);
    EPGraph second = graphStore.getGraph(secondGraph);

    EPGraph result = first.exclude(second);

    assertNotNull("resulting graph was null", result);

    long newGraphID = result.getId();

    assertEquals("wrong number of vertices", expectedVertexCount,
      result.getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      result.getEdgeCount());

    Collection<VertexData> vertexData = result.getVertices().collect();
    Collection<EdgeData> edgeData = result.getEdges().collect();

    assertEquals("wrong number of vertex values", expectedVertexCount,
      vertexData.size());
    assertEquals("wrong number of edge values", expectedEdgeCount,
      edgeData.size());

    for (VertexData v : vertexData) {
      assertTrue("vertex is not in new graph",
        v.getGraphs().contains(newGraphID));
    }

    for (EdgeData e : edgeData) {
      assertTrue("edge is not in new graph",
        e.getGraphs().contains(newGraphID));
    }
  }

  @Test
  public void testAssignment() throws Exception {
    EPGraph databaseCommunity = graphStore.getGraph(0L);
    EPGraph hadoopCommunity = graphStore.getGraph(1L);

    EPGraph newGraph = databaseCommunity.exclude(hadoopCommunity);

    Collection<VertexData> vertexData = newGraph.getVertices().collect();
    Collection<EdgeData> edgeData = newGraph.getEdges().collect();

    for (VertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      if (v.equals(alice)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(bob)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(eve)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }

    for (EdgeData e : edgeData) {
      Set<Long> gIDs = e.getGraphs();
      if (e.equals(edge0)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(edge1)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(edge6)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(edge21)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }
  }
}
