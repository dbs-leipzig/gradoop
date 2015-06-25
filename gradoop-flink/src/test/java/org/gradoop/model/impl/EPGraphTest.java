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

import com.google.common.collect.Sets;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EPGraphTest extends EPFlinkTest {

  @Test
  public void testGetVertices() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertEquals("wrong number of vertices", 11, dbGraph.getVertices().size());
  }

  @Test
  public void testGetEdges() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertEquals("wrong number of edges", 24, dbGraph.getEdges().size());

    assertEquals("wrong number of outgoing edges", 2,
      dbGraph.getOutgoingEdges(alice.getId()).size());

    assertEquals("wrong number of outgoing edges", 0,
      dbGraph.getOutgoingEdges(tagDatabases.getId()).size());

    assertEquals("wrong number of incoming edges", 3,
      dbGraph.getIncomingEdges(tagDatabases.getId()).size());

    assertEquals("wrong number of incoming edges", 0,
      dbGraph.getIncomingEdges(frank.getId()).size());
  }

  public void testMatch() throws Exception {

  }

  public void testProject() throws Exception {

  }

  public void testAggregate() throws Exception {

  }

  public void testSummarize() throws Exception {

  }

  @Test
  public void testCombine() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph databaseCommunity = graphStore.getGraph(0L);
    EPGraph graphCommunity = graphStore.getGraph(2L);

    EPGraph newGraph = graphCommunity.combine(databaseCommunity);

    assertEquals("wrong number of vertices", 5L, newGraph.getVertexCount());
    assertEquals("wrong number of edges", 8L, newGraph.getEdgeCount());

    Collection<EPVertexData> vertexData = newGraph.getVertices().collect();
    Collection<EPEdgeData> edgeData = newGraph.getEdges().collect();

    assertEquals("wrong number of vertex values", 5, vertexData.size());
    assertEquals("wrong number of edge values", 8, edgeData.size());

    // check if vertices are assigned to the new and the old graphs
    Long newGraphID = newGraph.getId();

    for (EPVertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      assertTrue("vertex was not in new graph", gIDs.contains(newGraphID));
      if (v.equals(alice)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(bob)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(carol)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(dave)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(eve)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }

    Set<Long> expectedIds = Sets.newHashSet(0L, 1L, 2L, 3L, 4L, 5L, 6L, 21L);

    for (EPEdgeData e : edgeData) {
      Long edgeID = e.getId();
      assertTrue("edge " + edgeID + "was not expected",
        expectedIds.contains(edgeID));
    }
  }

  @Test
  public void testOverlap() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph databaseCommunity = graphStore.getGraph(0L);
    EPGraph graphCommunity = graphStore.getGraph(2L);

    EPGraph newGraph = graphCommunity.overlap(databaseCommunity);

    assertEquals("wrong number of vertices", 2L, newGraph.getVertexCount());
    assertEquals("wrong number of edges", 2L, newGraph.getEdgeCount());

    Collection<EPVertexData> vertexData = newGraph.getVertices().collect();
    Collection<EPEdgeData> edgeData = newGraph.getEdges().collect();

    assertEquals("wrong number of vertex values", 2, vertexData.size());
    assertEquals("wrong number of edge values", 2, edgeData.size());

    // check if vertices are assigned to the new and the old graphs
    Long newGraphID = newGraph.getId();

    for (EPVertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      assertTrue("vertex was not in new graph", gIDs.contains(newGraphID));
      if (v.equals(alice)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(bob)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      }
    }

    Set<Long> expectedIds = Sets.newHashSet(0L, 1L);

    for (EPEdgeData e : edgeData) {
      Long edgeID = e.getId();
      assertTrue("edge " + edgeID + "was not expected",
        expectedIds.contains(edgeID));
    }
  }

  public void testExclude() throws Exception {

  }

  public void testCallForGraph() throws Exception {

  }

  public void testCallForCollection() throws Exception {

  }
}