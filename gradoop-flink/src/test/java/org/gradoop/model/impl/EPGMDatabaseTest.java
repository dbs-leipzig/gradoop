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
import org.gradoop.model.FlinkTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnitParamsRunner.class)
public class EPGMDatabaseTest extends FlinkTest {

  private EPGMDatabase graphStore;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public EPGMDatabaseTest() {
    graphStore = createSocialGraph();
  }

  @Test
  public void testGetDatabaseGraph() throws Exception {
    LogicalGraph dbGraph = graphStore.getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 11,
      dbGraph.getVertices().size());
    assertEquals("edge set has the wrong size", 24, dbGraph.getEdges().size());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      EPGMDatabaseTest.class.getResource("/sna_nodes").getFile();
    String edgeFile =
      EPGMDatabaseTest.class.getResource("/sna_edges").getFile();
    String graphFile =
      EPGMDatabaseTest.class.getResource("/sna_graphs").getFile();

    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore =
      EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile, env);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      databaseGraph = graphStore.getDatabaseGraph();

    assertEquals("Wrong vertex count", 11, databaseGraph.getVertexCount());
    assertEquals("Wrong edge count", 24, databaseGraph.getEdgeCount());
    assertEquals("Wrong graph count", 4,
      graphStore.getCollection().getGraphCount());
  }

  @Test
  public void testWriteAsJsonFile() throws Exception {
    String tmpDir = temporaryFolder.getRoot().toString();
    final String vertexFile = tmpDir + "/nodes.json";
    final String edgeFile = tmpDir + "/edges.json";
    final String graphFile = tmpDir + "/graphs.json";

    graphStore.writeAsJson(vertexFile, edgeFile, graphFile);

    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraphStore =
      EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile, env);

    assertEquals(graphStore.getDatabaseGraph().getVertexCount(),
      newGraphStore.getDatabaseGraph().getVertexCount());
    assertEquals(graphStore.getDatabaseGraph().getEdgeCount(),
      newGraphStore.getDatabaseGraph().getEdgeCount());
    assertEquals(graphStore.getCollection().getGraphCount(),
      newGraphStore.getCollection().getGraphCount());
  }
}
