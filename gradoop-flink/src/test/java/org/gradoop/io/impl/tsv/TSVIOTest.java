package org.gradoop.io.impl.tsv;

import org.gradoop.io.api.DataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.LogicalGraphTest;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import java.io.IOException;

public class TSVIOTest extends GradoopFlinkTestBase {

  @Test
  public void testTSVInput() throws Exception {
    String tsvFile =
      TSVIOTest.class.getResource("/data/tsv/tsvFile").getFile();

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TSVDataSource<>(tsvFile, config);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      graph = dataSource.getLogicalGraph();
    GradoopFlinkTestUtils.printLogicalGraph(graph);
  }

}
