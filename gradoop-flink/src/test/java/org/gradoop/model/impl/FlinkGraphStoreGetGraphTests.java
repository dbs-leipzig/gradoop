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

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class FlinkGraphStoreGetGraphTests extends EPFlinkTest {

  private EPGraphStore graphStore;

  public FlinkGraphStoreGetGraphTests() {
    graphStore = createSocialGraph();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][]{{0L, 3L, 4L}, {1L, 3L, 4L}, {2L, 4L, 6L}, {3L, 3L, 4L}});
  }

  @Parameterized.Parameter
  public long graphID;

  @Parameterized.Parameter(value = 1)
  public long expectedVertexCount;

  @Parameterized.Parameter(value = 2)
  public long expectedEdgeCount;

  @Test
  public void testGetGraph() throws Exception {
    EPGraph g = graphStore.getGraph(graphID);

    assertNotNull("graph was null", g);
    assertEquals("vertex set has the wrong size", expectedVertexCount,
      g.getVertices().size());
    assertEquals("edge set has the wrong size", expectedEdgeCount,
      g.getEdges().size());
  }

  @Test
  public void testGetDatabaseGraph() throws Exception {
    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 11,
      dbGraph.getVertices().size());
    assertEquals("edge set has the wrong size", 24, dbGraph.getEdges().size());
  }
}
