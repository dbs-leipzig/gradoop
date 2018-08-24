package org.gradoop.flink.io.impl.gdl;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GDLDataSinkTest extends GradoopFlinkTestBase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    FlinkAsciiGraphLoader testLoader = getSocialNetworkLoader();
    GraphCollection expectedCollection = testLoader.getGraphCollectionByVariables("g0","g1","g2","g3");

    String path = temporaryFolder.getRoot().getPath() + "/graph.gdl";
    DataSink gdlsink = new GDLDataSink(path);
    gdlsink.write(expectedCollection, true);
    getExecutionEnvironment().execute();

    FlinkAsciiGraphLoader sinkLoader = getLoaderFromFile(path);
    GraphCollection sinkCollection = sinkLoader
      .getGraphCollectionByVariables("g0","g1","g2","g3");

    collectAndAssertTrue(sinkCollection.equalsByGraphElementData(expectedCollection));
  }
}
