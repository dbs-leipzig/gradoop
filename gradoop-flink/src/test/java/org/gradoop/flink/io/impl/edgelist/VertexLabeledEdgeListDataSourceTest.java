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

public class VertexLabeledEdgeListDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testRead() throws Exception {
    String edgeListFile = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/edgelist/vertexlabeled/input").getFile();
    String gdlFile = VertexLabeledEdgeListDataSourceTest.class
      .getResource("/data/edgelist/vertexlabeled/expected.gdl").getFile();

    DataSource dataSource = new VertexLabeledEdgeListDataSource(edgeListFile, " ", "lan", config);
    LogicalGraph result = dataSource.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }
}
