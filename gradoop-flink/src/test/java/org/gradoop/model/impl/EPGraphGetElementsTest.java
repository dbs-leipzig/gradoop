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
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class EPGraphGetElementsTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphGetElementsTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  @Parameters({"-1, 11", "0, 3", "1, 3", "2, 4", "3, 3"})
  public void testGetVertices(long graphID, long expectedVertexCount) throws
    Exception {
    EPGraph g = (graphID == -1) ? graphStore.getDatabaseGraph() :
      graphStore.getGraph(graphID);

    assertNotNull("graph was null", g);
    assertEquals("wrong number of vertices", expectedVertexCount,
      g.getVertices().size());
    assertEquals("wrong number of vertices", expectedVertexCount,
      g.getVertexCount());
  }

  @Test
  @Parameters({"-1, 24", "0, 4", "1, 4", "2, 6", "3, 4"})
  public void testGetEdges(long graphID, long expectedEdgeCount) throws
    Exception {
    EPGraph g = (graphID == -1) ? graphStore.getDatabaseGraph() :
      graphStore.getGraph(graphID);

    assertNotNull("graph was null", g);
    assertEquals("wrong number of edges", expectedEdgeCount,
      g.getEdges().size());
    assertEquals("wrong number of edges", expectedEdgeCount, g.getEdgeCount());
  }

  @Test
  @Parameters({"-1, 0, 2, 4", // vertex 0 in db graph
    "0, 0, 1, 2", // vertex 0 in logical graph 0
    "2, 0, 1, 1", // vertex 0 in logical graph 2
    "-1, 6, 0, 3", // vertex 6 with no outgoing sna_edges
    "-1, 9, 5, 0", // vertex 9 with no incoming sna_edges
  })
  public void testGetOutgoingAndIncomingEdges(long graphID, long vertexID,
    long expectedOutgoingEdgeCount, long expectedIncomingEdgeCount) throws
    Exception {
    EPGraph g = (graphID == -1) ? graphStore.getDatabaseGraph() :
      graphStore.getGraph(graphID);

    EPEdgeCollection outgoingEdges = g.getOutgoingEdges(vertexID);
    EPEdgeCollection incomingEdges = g.getIncomingEdges(vertexID);

    assertNotNull("outgoing edge collection was null", outgoingEdges);
    assertNotNull("incoming edge collection was null", incomingEdges);

    assertEquals("wrong number of outgoing edges", expectedOutgoingEdgeCount,
      outgoingEdges.size());
    assertEquals("wrong number of incoming edges", expectedIncomingEdgeCount,
      incomingEdges.size());
  }
}
