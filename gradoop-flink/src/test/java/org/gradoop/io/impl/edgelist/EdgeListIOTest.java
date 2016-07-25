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

package org.gradoop.io.impl.edgelist;

import org.gradoop.io.api.DataSource;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EdgeListIOTest extends GradoopFlinkTestBase {

  @Test
  public void testEdgeListData() throws Exception {
    String edgeListFile =
            EdgeListIOTest.class.getResource("/data/edgelist/input")
              .getFile();

    String gdlFile =
            EdgeListIOTest.class.getResource("/data/edgelist/expected.gdl")
              .getFile();

    // load from tsv file
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
            new EdgeListDataSource<>(edgeListFile, " ", "lan", config);

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
