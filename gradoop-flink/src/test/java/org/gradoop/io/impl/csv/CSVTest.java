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

package org.gradoop.io.impl.csv;

import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.csv.parser.XmlParser;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
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
    String xsdFile =
      CSVTest.class.getResource("/data/csv/csv_format.xsd").getFile();
    String delimiter = "|";

//    System.out.println(XmlParser.validateXml(xmlFile, xsdFile));

//    XmlParser.parseSAX(xsdFile);
    XmlParser.parseDom2(xsdFile);


    // create datasource
    DataSource<GraphHeadPojo, VertexPojo, EdgePojo> dataSource =
      new CSVDataSource<>(config, xmlFile, delimiter);
    //get transactions
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      dataSource.getLogicalGraph();


  }

}
