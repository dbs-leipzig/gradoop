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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class CSVDataSourceTest extends GradoopFlinkTestBase {

  /**
   * Test method for
   *
   * {@link CSVDataSource#getLogicalGraph()} ()}
   * @throws Exception
   */
  @Test
  public void testRead() throws Exception {

    // paths to input files
    String csvFiles = CSVDataSourceTest.class.getResource("/data/csv/").getPath();

    String metaXmlFile =
      CSVDataSourceTest.class.getResource("/data/csv/test2.xml").getFile();

    // create datasource
    DataSource dataSource = new CSVDataSource(metaXmlFile, csvFiles,config);

    // get collection
    GraphCollection collection = dataSource.getGraphCollection();


    Collection<GraphHead> graphHeads = Lists.newArrayList();
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();

    System.out.println("collection = " + collection.getGraphHeads().count());
    collection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(graphHeads));
    collection.getVertices()
      .output(new LocalCollectionOutputFormat<>(vertices));
    collection.getEdges()
      .output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    for (Iterator<GraphHead> iterator = graphHeads.iterator(); iterator.hasNext(); ) {
      GraphHead next =  iterator.next();
      System.out.println("next = " + next.getPropertyValue("key").getString() + "   -    " +next);
    }
//
//    for (Iterator<Vertex> iterator = vertices.iterator(); iterator.hasNext(); ) {
//      Vertex next =  iterator.next();
//      System.out.println("VERTEX = " + next.getPropertyValue("graphs").getString());
//    }

//    for (Iterator<Edge> iterator = edges.iterator(); iterator.hasNext(); ) {
//      Edge next =  iterator.next();
//      System.out.println("EDGE = " + next.getPropertyValue("key").getString());
//    }
    assertEquals("Wrong graph count", 9, graphHeads.size());
//    assertEquals("Wrong vertex count", 11, vertices.size());
//    assertEquals("Wrong edge count", 24, edges.size());

  }

}
