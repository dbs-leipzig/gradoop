/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.impl.tsv;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.io.api.DataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;

import static org.gradoop.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.GradoopTestUtils.validateEPGMGraphElementCollections;
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

  @Test
  public void testTSVData() throws Exception {
    String tsvFile =
            TSVIOTest.class.getResource("/data/tsv/tsvFile").getFile();

    String gdlFile =
            TSVIOTest.class.getResource("/data/tsv/tsv.gdl").getFile();

    // load from tsv file
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
            new TSVDataSource<>(tsvFile, config);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
            tsvGraph = dataSource.getLogicalGraph();

    // load from gdl
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
            getLoaderFromFile(gdlFile);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> resultGraph =
            loader.getLogicalGraphByVariable("result");

    // test element data
    collectAndAssertTrue(resultGraph.equalsByElementData(tsvGraph));

  }
}
