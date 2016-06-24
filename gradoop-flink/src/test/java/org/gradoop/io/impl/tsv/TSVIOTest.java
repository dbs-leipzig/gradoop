package org.gradoop.io.impl.tsv;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
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
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TSVIOTest extends GradoopFlinkTestBase {

  @Test
  public void testTSVInput() throws Exception {
    String tsvFile =
      TSVIOTest.class.getResource("/data/tsv/tsvFile").getFile();

    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new TSVDataSource<>(tsvFile, config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection = dataSource.getGraphCollection();

    Collection<GraphHeadPojo> graphHeads = Lists.newArrayList();
    Collection<VertexPojo> vertices = Lists.newArrayList();
    Collection<EdgePojo> edges = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));



    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 1, graphHeads.size());
    assertEquals("Wrong vertex count", 20, vertices.size());
    assertEquals("Wrong edge count", 12, edges.size());
  }

}
