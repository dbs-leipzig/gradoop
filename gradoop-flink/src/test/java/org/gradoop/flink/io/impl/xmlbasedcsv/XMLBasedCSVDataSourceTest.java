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

package org.gradoop.flink.io.impl.xmlbasedcsv;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class XMLBasedCSVDataSourceTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link XMLBasedCSVDataSource#getLogicalGraph()} ()}
   * @throws Exception
   */
  @Test
  public void testFirstRead() throws Exception {

    // paths to input files
    String csvFiles = XMLBasedCSVDataSourceTest.class.getResource("/data/xmlbasedcsv/").getPath();

    String metaXmlFile = XMLBasedCSVDataSourceTest.class.getResource("/data/xmlbasedcsv/test1.xml")
      .getFile();

    // create datasource
    DataSource dataSource = new XMLBasedCSVDataSource(metaXmlFile, csvFiles,config);

    // get collection
    GraphCollection collection = dataSource.getGraphCollection();

    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 3, graphHeads.size());
    assertEquals("Wrong vertex count", 11, vertices.size());
    assertEquals("Wrong edge count", 20, edges.size());
  }

  /**
   * Test method for
   *
   * {@link XMLBasedCSVDataSource#getLogicalGraph()} ()}
   * @throws Exception
   */
  @Test
  public void testSecondRead() throws Exception {

    // paths to input files
    String csvFiles = XMLBasedCSVDataSourceTest.class.getResource("/data/xmlbasedcsv/").getPath();

    String metaXmlFile = XMLBasedCSVDataSourceTest.class.getResource("/data/xmlbasedcsv/test2.xml")
      .getFile();

    // create datasource
    DataSource dataSource = new XMLBasedCSVDataSource(metaXmlFile, csvFiles,config);

    // get collection
    GraphCollection collection = dataSource.getGraphCollection();

    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 9, graphHeads.size());
    assertEquals("Wrong vertex count", 25, vertices.size());
    assertEquals("Wrong edge count", 29, edges.size());
  }

}
