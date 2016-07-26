package org.gradoop.io.impl.dot;

import org.apache.flink.api.java.DataSet;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.json.JSONDataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class DotIOTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDotDataSink() throws Exception {

    String vertexFile =
      DotIOTest.class.getResource("/data/json/sna/nodes.json").getFile();
    String edgeFile =
      DotIOTest.class.getResource("/data/json/sna/edges.json").getFile();
    String graphFile =
      DotIOTest.class.getResource("/data/json/sna/graphs.json").getFile();

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new JSONDataSource<>(graphFile, vertexFile, edgeFile, config);

    String tmpDir = temporaryFolder.getRoot().toString();

    final String dotFile = tmpDir + "/test.dot";

    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new DotDataSink<>(dotFile);

    dataSink.write(dataSource.getGraphTransactions());

    getExecutionEnvironment().execute();

    DataSet<String> input = getExecutionEnvironment().readTextFile(dotFile);

    assertEquals("wrong number of lines", 51, input.count());
  }
}
