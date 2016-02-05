package org.gradoop.model.impl.operators.tostring;

import org.apache.commons.io.FileUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class CanonicalAdjacencyMatrixBuilderTest extends GradoopFlinkTestBase {

  @Test
  public void textExecute() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader = new
      FlinkAsciiGraphLoader<>(getConfig());

    loader.initDatabaseFromFile(CanonicalAdjacencyMatrixBuilderTest.class
        .getResource("/data/gdl/cam_test.gdl").getFile());

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> g = loader
      .getDatabase().getCollection();

    CanonicalAdjacencyMatrixBuilder<GraphHeadPojo, VertexPojo, EdgePojo> cam =
      new CanonicalAdjacencyMatrixBuilder<>(
        new GraphHeadToDataString<GraphHeadPojo>(),
        new VertexToDataString<VertexPojo>(),
        new EdgeToDataString<EdgePojo>()
      );

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(CanonicalAdjacencyMatrixBuilderTest.class
        .getResource("/data/expected/cam_test").getFile()));

    assertTrue(expectation.equals(result));
  }

}