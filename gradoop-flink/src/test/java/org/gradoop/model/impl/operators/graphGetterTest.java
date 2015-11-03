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

import org.gradoop.GradoopTestBaseUtils;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class graphGetterTest extends FlinkTestBase {

  public graphGetterTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testGetVertices() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();

    assertNotNull("graph was null", g);
    assertEquals("wrong number of vertices", 11L, g.getVertices().count());
    assertEquals("wrong number of vertices", 11L, g.getVertexCount());
  }

  @Test
  public void testGetEdges() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();

    assertNotNull("graph was null", g);
    assertEquals("wrong number of edges", 24L, g.getEdges().count());
    assertEquals("wrong number of edges", 24L, g.getEdgeCount());
  }

  @Test
  public void testGetOutgoingEdges() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();
    assertNotNull("graph was null", g);
    assertEquals("wrong number of outgoing edges", 2L,
      g.getOutgoingEdges(GradoopTestBaseUtils.VERTEX_PERSON_ALICE.getId())
        .count());
  }

  @Test
  public void testGetOutgoingEdges2() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();
    assertNotNull("graph was null", g);
    assertEquals("wrong number of outgoing edges", 0L,
      g.getOutgoingEdges(GradoopTestBaseUtils.VERTEX_TAG_DATABASES.getId())
        .count());
  }

  @Test
  public void testIncomingEdges() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();
    assertNotNull("graph was null", g);
    assertEquals("wrong number of outgoing edges", 4,
      g.getIncomingEdges(GradoopTestBaseUtils.VERTEX_PERSON_ALICE.getId())
        .count());
  }

  @Test
  public void testIncomingEdges2() throws Exception {
    LogicalGraph g = getGraphStore().getDatabaseGraph();
    assertNotNull("graph was null", g);
    assertEquals("wrong number of outgoing edges", 0L,
      g.getIncomingEdges(GradoopTestBaseUtils.VERTEX_FORUM_GDBS.getId())
        .count());
  }
}
