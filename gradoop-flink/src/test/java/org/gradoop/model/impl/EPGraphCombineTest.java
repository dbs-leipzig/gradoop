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
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class EPGraphCombineTest extends EPFlinkTest {

  private EPGraphStore graphStore;

  public EPGraphCombineTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"0, 0, 3, 4", "0, 2, 5, 8", // same graph
    "0, 2, 5, 8", // overlapping
    "2, 0, 5, 8", // overlapping switched
    "0, 1, 6, 8", // non-overlapping
    "1, 0, 6, 8"} // non-overlapping switched
  )
  public void testCombine(long firstGraph, long secondGraph,
    long expectedVertexCount, long expectedEdgeCount) throws Exception {
    EPGraph first = graphStore.getGraph(firstGraph);
    EPGraph second = graphStore.getGraph(secondGraph);

    EPGraph result = first.combine(second);

    assertNotNull("resulting graph was null", result);

    long newGraphID = result.getId();

    assertEquals("wrong number of vertices", expectedVertexCount,
      result.getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      result.getEdgeCount());

    Collection<EPVertexData> vertexData = result.getVertices().collect();
    Collection<EPEdgeData> edgeData = result.getEdges().collect();

    assertEquals("wrong number of vertex values", expectedVertexCount,
      vertexData.size());
    assertEquals("wrong number of edge values", expectedEdgeCount,
      edgeData.size());

    for (EPVertexData v : vertexData) {
      assertTrue("vertex is not in new graph",
        v.getGraphs().contains(newGraphID));
    }

    for (EPEdgeData e : edgeData) {
      assertTrue("edge is not in new graph",
        e.getGraphs().contains(newGraphID));
    }
  }

  @Test
  public void testAssignment() throws Exception {
    EPGraph databaseCommunity = graphStore.getGraph(0L);
    EPGraph graphCommunity = graphStore.getGraph(1L);

    EPGraph newGraph = graphCommunity.combine(databaseCommunity);

    Collection<EPVertexData> vertexData = newGraph.getVertices().collect();
    Collection<EPEdgeData> edgeData = newGraph.getEdges().collect();

    for (EPVertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      if (v.equals(alice)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(bob)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(eve)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (v.equals(carol)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (v.equals(dave)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (v.equals(frank)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }

    for (EPEdgeData e : edgeData) {
      Set<Long> gIDs = e.getGraphs();
      if (e.equals(edge0)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(edge1)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(edge6)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(edge21)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(edge4)) {
        assertEquals("wrong number of graphs", 4, gIDs.size());
      } else if (e.equals(edge5)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(edge22)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(edge23)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }
  }
}
