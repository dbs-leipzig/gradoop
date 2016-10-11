package org.gradoop.flink.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;

public class JSONDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
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
