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

package org.gradoop.flink.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JSONIOTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testGetDatabaseGraph() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
      .getDatabase().getDatabaseGraph();

    assertNotNull("database graph was null", dbGraph);

    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    dbGraph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    dbGraph.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("vertex set has the wrong size", 11, vertices.size());
    assertEquals("edge set has the wrong size", 24, edges.size());
  }

  @Test
  public void testFromJsonFile() throws Exception {
    String vertexFile =
      JSONIOTest.class.getResource("/data/json/sna/nodes.json").getFile();
    String edgeFile =
      JSONIOTest.class.getResource("/data/json/sna/edges.json").getFile();
    String graphFile =
      JSONIOTest.class.getResource("/data/json/sna/graphs.json").getFile();

    DataSource dataSource = new JSONDataSource(
      graphFile, vertexFile, edgeFile, config);

    GraphCollection
      collection = dataSource.getGraphCollection();

    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

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

    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    // write to JSON
    loader.getDatabase().writeTo(
      new JSONDataSink(graphFile, vertexFile, edgeFile, getConfig()));

    getExecutionEnvironment().execute();

    // read from JSON
    GraphCollection collection = new JSONDataSource(
      graphFile, vertexFile, edgeFile, getConfig()).getGraphCollection();

    Collection<GraphHead> expectedGraphHeads  = loader.getGraphHeads();
    Collection<Vertex>    expectedVertices    = loader.getVertices();
    Collection<Edge>      expectedEdges       = loader.getEdges();

    Collection<GraphHead> loadedGraphHeads    = Lists.newArrayList();
    Collection<Vertex>    loadedVertices      = Lists.newArrayList();
    Collection<Edge>      loadedEdges         = Lists.newArrayList();

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
