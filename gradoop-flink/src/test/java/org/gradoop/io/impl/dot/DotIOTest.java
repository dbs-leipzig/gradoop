package org.gradoop.io.impl.dot;

import org.apache.flink.api.java.DataSet;
import org.gradoop.io.api.DataSink;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class DotIOTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLogicalGraphAsDot() throws Exception {

    String gdlFile =
      DotIOTest.class.getResource("/data/dot/input.gdl").getFile();

    // load from gdl
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromFile(gdlFile);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> resultGraph =
      loader.getLogicalGraphByVariable("singleGraph");

    String tmpDir = temporaryFolder.getRoot().toString();

    final String dotFile = tmpDir + "/test.dot";

    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new DotDataSink<>(dotFile, false);

    dataSink.write(resultGraph);

    getExecutionEnvironment().execute();

    DataSet<String> input = getExecutionEnvironment().readTextFile(dotFile);

    assertEquals("wrong number of lines", 51, input.count());
  }
}
