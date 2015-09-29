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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.FlinkTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class EPGMDatabaseJSONTest extends FlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public EPGMDatabaseJSONTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testGetDatabaseGraph() throws Exception {
    LogicalGraph dbGraph = getGraphStore().getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 11,
      dbGraph.getVertices().size());
    assertEquals("edge set has the wrong size", 24, dbGraph.getEdges().size());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      EPGMDatabaseJSONTest.class.getResource("/data/sna_nodes").getFile();
    String edgeFile =
      EPGMDatabaseJSONTest.class.getResource("/data/sna_edges").getFile();
    String graphFile =
      EPGMDatabaseJSONTest.class.getResource("/data/sna_graphs").getFile();

    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile,
      ExecutionEnvironment.getExecutionEnvironment());

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

    getGraphStore().writeAsJson(vertexFile, edgeFile, graphFile);

    EPGMDatabase<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile,
      ExecutionEnvironment.getExecutionEnvironment());

    assertEquals(getGraphStore().getDatabaseGraph().getVertexCount(),
      newGraphStore.getDatabaseGraph().getVertexCount());
    assertEquals(getGraphStore().getDatabaseGraph().getEdgeCount(),
      newGraphStore.getDatabaseGraph().getEdgeCount());
    assertEquals(getGraphStore().getCollection().getGraphCount(),
      newGraphStore.getCollection().getGraphCount());
  }
}
