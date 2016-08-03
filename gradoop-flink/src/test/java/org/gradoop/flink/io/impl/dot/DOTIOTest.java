package org.gradoop.flink.io.impl.dot;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DOTIOTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLogicalGraphAsDot() throws Exception {

    String gdlFile =
      DOTIOTest.class.getResource("/data/dot/input.gdl").getFile();

    // load from gdl
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);

    // load input graph
    LogicalGraph inputGraph = loader.getLogicalGraphByVariable("singleGraph");

    // create temp directory
    String tmpDir = temporaryFolder.getRoot().toString();

    final String dotFile = tmpDir + "/test.dot";

    // create datasink
    DataSink dataSink = new DOTDataSink(dotFile, false);

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
