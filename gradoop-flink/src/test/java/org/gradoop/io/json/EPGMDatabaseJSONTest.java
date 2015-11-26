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

package org.gradoop.io.json;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class EPGMDatabaseJSONTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public EPGMDatabaseJSONTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testGetDatabaseGraph() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
      .getDatabase().getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);
    assertEquals("vertex set has the wrong size", 11,
      dbGraph.getVertices().count());
    assertEquals("edge set has the wrong size", 24, dbGraph.getEdges().count());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/nodes.json").getFile();
    String edgeFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/edges.json").getFile();
    String graphFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/graphs.json").getFile();

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo>
      graphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile,
      ExecutionEnvironment.getExecutionEnvironment());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
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

    getSocialNetworkLoader().getDatabase()
      .writeAsJson(vertexFile, edgeFile, graphFile);

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraphStore = EPGMDatabase.fromJsonFile(vertexFile, edgeFile, graphFile,
      ExecutionEnvironment.getExecutionEnvironment());

    assertEquals(
      getSocialNetworkLoader().getDatabase().getDatabaseGraph().getVertexCount(),
      newGraphStore.getDatabaseGraph().getVertexCount()
    );
    assertEquals(
      getSocialNetworkLoader().getDatabase().getDatabaseGraph().getEdgeCount(),
      newGraphStore.getDatabaseGraph().getEdgeCount()
    );
    assertEquals(
      getSocialNetworkLoader().getDatabase().getCollection().getGraphCount(),
      newGraphStore.getCollection().getGraphCount()
    );
  }
}
