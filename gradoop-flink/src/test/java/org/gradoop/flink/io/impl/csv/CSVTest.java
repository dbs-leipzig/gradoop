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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.junit.Test;


public class CSVTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link CSVDataSource#getLogicalGraph()} ()}
   * @throws Exception
   */
  @Test
  public void testRead() throws Exception {

    String xmlFile =
      CSVTest.class.getResource("/data/csv/test_meta.xml").getFile();
    String delimiter = "|";

    // create datasource
    DataSource dataSource = new CSVDataSource(config, xmlFile, delimiter);
    //get transactions
    LogicalGraph graph = dataSource.getLogicalGraph();


  }

}
