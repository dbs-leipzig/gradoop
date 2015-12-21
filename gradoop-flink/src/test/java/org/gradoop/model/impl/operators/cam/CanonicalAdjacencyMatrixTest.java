package org.gradoop.model.impl.operators.cam;

import org.apache.commons.io.FileUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.cam.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.VertexDataLabeler;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class CanonicalAdjacencyMatrixTest extends GradoopFlinkTestBase {

  @Test
  public void textExecute() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader = new
      FlinkAsciiGraphLoader<>(getConfig());

    loader.initDatabaseFromFile(CanonicalAdjacencyMatrixTest.class
        .getResource("/data/gdl/cam_test.gdl").getFile());

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> g = loader
      .getDatabase().getCollection();

    CanonicalAdjacencyMatrix<GraphHeadPojo, VertexPojo, EdgePojo> cam =
      new CanonicalAdjacencyMatrix<>(
        new GraphHeadDataLabeler<GraphHeadPojo>(),
        new VertexDataLabeler<VertexPojo>(),
        new EdgeDataLabeler<EdgePojo>()
      );

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(CanonicalAdjacencyMatrixTest.class
        .getResource("/data/output/cam_test").getFile()));

    assertTrue(expectation.equals(result));
  }

}