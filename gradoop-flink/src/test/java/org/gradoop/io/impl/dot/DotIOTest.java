package org.gradoop.io.impl.dot;

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

import java.util.List;

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

    // load input graph
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> inputGraph =
      loader.getLogicalGraphByVariable("singleGraph");

    // create temp directory
    String tmpDir = temporaryFolder.getRoot().toString();

    final String dotFile = tmpDir + "/test.dot";

    // create datasink
    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> dataSink =
      new DotDataSink<>(dotFile, false);

    // write graph
    dataSink.write(inputGraph);

    // execute
    getExecutionEnvironment().execute();

    int vertexLines = 0;
    int edgeLines = 0;

    // read written file
    List<String> dotLines = getExecutionEnvironment()
      .readTextFile(dotFile)
      .collect();

    // count vertex and edge lines
    for (String line : dotLines){

      if (line.contains("->")){
        edgeLines++;
      } else if (
          !line.contains("di") &&
          !line.contains("{") &&
          !line.contains("}")){
        vertexLines++;
      }
    }

    // assert
    assertEquals("Wrong number of edge lines", 4, edgeLines);
    assertEquals("Wrong number of vertex lines", 3, vertexLines);
 }
}
