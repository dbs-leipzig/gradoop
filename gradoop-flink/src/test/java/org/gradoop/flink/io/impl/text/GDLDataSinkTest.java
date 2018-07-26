package org.gradoop.flink.io.impl.text;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GDLDataSinkTest extends GradoopFlinkTestBase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String gdlPath = GDLDataSinkTest.class
      .getResource("/data/text/socialgraph.gdl")
      .getFile();

    LogicalGraph expected = getLoaderFromFile(gdlPath)
      .getLogicalGraphByVariable("g1");

    expected.print();

    String path = temporaryFolder.getRoot().getPath() + "/graph.gdl";
    expected.writeTo(new GDLDataSink(path));

    String logicalGraphVariable = "g" + expected.getGraphHead().collect().get(0).getId().toString();

    LogicalGraph sinkGraph = getLoaderFromFile(path)
      .getLogicalGraphByVariable(logicalGraphVariable);

    collectAndAssertTrue(sinkGraph.equalsByElementData(expected));
  }
}
