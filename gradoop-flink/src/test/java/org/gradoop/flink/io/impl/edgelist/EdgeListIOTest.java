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

package org.gradoop.flink.io.impl.edgelist;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EdgeListIOTest extends GradoopFlinkTestBase {

  @Test
  public void testEdgeListData() throws Exception {
    String edgeListFile = EdgeListIOTest.class
      .getResource("/data/edgelist/input").getFile();

    String gdlFile = EdgeListIOTest.class
      .getResource("/data/edgelist/expected.gdl").getFile();

    // load from tsv file
    DataSource dataSource = new EdgeListDataSource(edgeListFile, " ", "lan", config);

    LogicalGraph tsvGraph = dataSource.getLogicalGraph();

    // load from gdl
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);

    LogicalGraph resultGraph = loader.getLogicalGraphByVariable("result");

    // test element data
    collectAndAssertTrue(resultGraph.equalsByElementData(tsvGraph));

  }
}
