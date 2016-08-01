package org.gradoop.model.impl.operators.tostring;

import org.apache.commons.io.FileUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class CanonicalAdjacencyMatrixBuilderTest extends GradoopFlinkTestBase {

  @Test
  public void testDirected() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(CanonicalAdjacencyMatrixBuilderTest.class
        .getResource("/data/gdl/cam_test.gdl").getFile());

    GraphCollection g = loader.getDatabase().getCollection();

    CanonicalAdjacencyMatrixBuilder cam =
      new CanonicalAdjacencyMatrixBuilder(
        new GraphHeadToDataString<>(),
        new VertexToDataString<>(),
        new EdgeToDataString<>(), true);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(CanonicalAdjacencyMatrixBuilderTest.class
        .getResource("/data/expected/cam_test_directed").getFile()));

    assertTrue(expectation.equals(result));
  }

  @Test
  public void testUndirected() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(CanonicalAdjacencyMatrixBuilderTest.class
      .getResource("/data/gdl/cam_test.gdl").getFile());

    GraphCollection g = loader.getDatabase().getCollection();

    CanonicalAdjacencyMatrixBuilder cam =
      new CanonicalAdjacencyMatrixBuilder(
        new GraphHeadToDataString<>(),
        new VertexToDataString<>(),
        new EdgeToDataString<>(), false);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(CanonicalAdjacencyMatrixBuilderTest.class
        .getResource("/data/expected/cam_test_undirected").getFile()));

    assertTrue(expectation.equals(result));
  }

}