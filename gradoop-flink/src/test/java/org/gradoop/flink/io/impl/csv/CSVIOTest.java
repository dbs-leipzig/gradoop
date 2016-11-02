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
import org.gradoop.flink.io.impl.csv.parser.ObjectFactory;
import org.gradoop.flink.io.impl.csv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;

public class CSVIOTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link CSVDataSource#getLogicalGraph()} ()}
   * @throws Exception
   */
  @Test
  public void readTestWithoutGraph() throws Exception {

    // paths to input files
    String csvFiles = CSVIOTest.class.getResource("/data/csv/").getPath();

    String metaXmlFile =
      CSVIOTest.class.getResource("/data/csv/test2.xml").getFile();

    // create datasource
    DataSource dataSource = new CSVDataSource(metaXmlFile, csvFiles,config);

    // get collection
    GraphCollection graph = dataSource.getGraphCollection();

    GradoopFlinkTestUtils.printGraphCollection(graph);
  }

}
