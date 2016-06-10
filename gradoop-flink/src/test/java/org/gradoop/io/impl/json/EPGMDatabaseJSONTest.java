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

package org.gradoop.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.io.api.DataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;

import static org.gradoop.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGMDatabaseJSONTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testGetDatabaseGraph() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> dbGraph =
      getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);

    Collection<VertexPojo> vertices = Lists.newArrayList();
    Collection<EdgePojo> edges = Lists.newArrayList();

    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("vertex set has the wrong size", 11, vertices.size());
    assertEquals("edge set has the wrong size", 24, edges.size());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/nodes.json").getFile();
    String edgeFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/edges.json").getFile();
    String graphFile =
      EPGMDatabaseJSONTest.class.getResource("/data/json/sna/graphs.json").getFile();

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new JSONDataSource<>(graphFile, vertexFile, edgeFile, config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection = dataSource.getGraphCollection();

    Collection<GraphHeadPojo> graphHeads = Lists.newArrayList();
    Collection<VertexPojo> vertices = Lists.newArrayList();
    Collection<EdgePojo> edges = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 4, graphHeads.size());
    assertEquals("Wrong vertex count", 11, vertices.size());
    assertEquals("Wrong edge count", 24, edges.size());
  }

  @Test
  public void testWriteAsJsonFile() throws Exception {
    String tmpDir = temporaryFolder.getRoot().toString();
    final String vertexFile = tmpDir + "/nodes.json";
    final String edgeFile   = tmpDir + "/edges.json";
    final String graphFile  = tmpDir + "/graphs.json";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    // write to JSON
    loader.getDatabase()
      .writeTo(new JSONDataSink<>(graphFile, vertexFile, edgeFile, getConfig()));

    getExecutionEnvironment().execute();

    // read from JSON
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      new JSONDataSource<>(graphFile, vertexFile, edgeFile, getConfig())
        .getGraphCollection();

    Collection<GraphHeadPojo> expectedGraphHeads  = loader.getGraphHeads();
    Collection<VertexPojo>    expectedVertices    = loader.getVertices();
    Collection<EdgePojo>      expectedEdges       = loader.getEdges();

    Collection<GraphHeadPojo> loadedGraphHeads    = Lists.newArrayList();
    Collection<VertexPojo>    loadedVertices      = Lists.newArrayList();
    Collection<EdgePojo>      loadedEdges         = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(expectedGraphHeads, loadedGraphHeads);
    validateEPGMElementCollections(expectedVertices, loadedVertices);
    validateEPGMGraphElementCollections(expectedVertices, loadedVertices);
    validateEPGMElementCollections(expectedEdges, loadedEdges);
    validateEPGMGraphElementCollections(expectedEdges, loadedEdges);
  }
}
