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
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
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

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0 {key : \"MySQL_GraphDomain_GraphHead:2\"}[" +
        "(v000:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Axel\",key : \"MySQL_ComputerScience_Person:0\",age: 23})" +
        "(v001:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Maria\",key : \"MySQL_ComputerScience_Person:1\",age: 25})" +
        "(v002:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Lisa\",key : \"MySQL_ComputerScience_Person:2\",age: 23})" +
        "(v003:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Stefan\",key : \"MySQL_ComputerScience_Person:3\",age: 24})" +
        "(v004:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Mathias\",key : \"MySQL_ComputerScience_Person:4\",age: 24})" +
        "(v005:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Max\",key : \"MySQL_ComputerScience_Person:5\",age: 20})" +
        "(v006:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Lisa\",key : \"MySQL_ComputerScience_Person:6\",age: 18})" +
        "(v007:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Matti\",key : \"MySQL_ComputerScience_Person:7\",age: 24})" +
        "(v008:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Erik\",key : \"MySQL_ComputerScience_Person:8\",age: 24})" +
        "(v009:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Sven\",key : \"MySQL_ComputerScience_Person:9\",age: 23})" +
        "(v010:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Bea\",key : \"MySQL_ComputerScience_Person:10\",age: 26})" +
        "(v000)-" +
          "[e000:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:0\"," +
          "key : \"MySQL_ComputerScience_isMember:01@\"," +
          "since : 2011," +
          "target : \"MySQL_ComputerScience_Person:1\"}]" +
        "->(v001)" +
        "(v001)-" +
        "[e001:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:1\"," +
          "key : \"MySQL_ComputerScience_isMember:10@\"," +
          "since : 2012," +
          "target : \"MySQL_ComputerScience_Person:0\"}]" +
        "->(v000)" +
        "(v002)-" +
          "[e002:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:2\"," +
          "key : \"MySQL_ComputerScience_isMember:21@\"," +
          "since : 2013," +
          "target : \"MySQL_ComputerScience_Person:1\"}]" +
        "->(v001)" +
        "(v003)-" +
          "[e003:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:3\"," +
          "key : \"MySQL_ComputerScience_isMember:32@\"," +
          "since : 2014," +
          "target : \"MySQL_ComputerScience_Person:2\"}]" +
        "->(v002)" +
        "(v004)-" +
          "[e004:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:4\"," +
          "key : \"MySQL_ComputerScience_isMember:43@\"," +
          "since : 2015," +
          "target : \"MySQL_ComputerScience_Person:3\"}]" +
        "->(v003)" +
        "(v005)-" +
          "[e005:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:5\"," +
          "key : \"MySQL_ComputerScience_isMember:54@\"," +
          "since : 2016," +
          "target : \"MySQL_ComputerScience_Person:4\"}]" +
        "->(v004)" +
        "(v006)-" +
          "[e006:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:6\"," +
          "key : \"MySQL_ComputerScience_isMember:65@\"," +
          "since : 2005," +
          "target : \"MySQL_ComputerScience_Person:5\"}]" +
        "->(v005)" +
        "(v007)-" +
          "[e007:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:7\"," +
          "key : \"MySQL_ComputerScience_isMember:76@\"," +
          "since : 2006," +
          "target : \"MySQL_ComputerScience_Person:6\"}]" +
        "->(v006)" +
        "(v008)-" +
          "[e008:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:8\"," +
          "key : \"MySQL_ComputerScience_isMember:87@\"," +
          "since : 2010," +
          "target : \"MySQL_ComputerScience_Person:7\"}]" +
        "->(v007)" +
        "(v009)-" +
          "[e009:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:9\"," +
          "key : \"MySQL_ComputerScience_isMember:98@\"," +
          "since : 2008," +
          "target : \"MySQL_ComputerScience_Person:8\"}]" +
        "->(v008)" +
        "(v000)-" +
          "[e010:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:0\"," +
          "key : \"MySQL_ComputerScience_isMember:09@\"," +
          "since : 2011," +
          "target : \"MySQL_ComputerScience_Person:9\"}]" +
        "->(v009)" +
        "(v001)-" +
          "[e011:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:1\"," +
          "key : \"MySQL_ComputerScience_isMember:18@\"," +
          "since : 2012," +
          "target : \"MySQL_ComputerScience_Person:8\"}]" +
        "->(v008)" +
        "(v002)-" +
          "[e012:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:2\"," +
          "key : \"MySQL_ComputerScience_isMember:27@\"," +
          "since : 2006," +
          "target : \"MySQL_ComputerScience_Person:7\"}]" +
        "->(v007)" +
        "(v003)-" +
          "[e013:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:3\"," +
          "key : \"MySQL_ComputerScience_isMember:36@\"," +
          "since : 2004," +
          "target : \"MySQL_ComputerScience_Person:6\"}]" +
        "->(v006)" +
        "(v004)-" +
          "[e014:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:4\"," +
          "key : \"MySQL_ComputerScience_isMember:45@\"," +
          "since : 2011," +
          "target : \"MySQL_ComputerScience_Person:5\"}]" +
        "->(v005)" +
        "(v005)-" +
          "[e015:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:5\"," +
          "key : \"MySQL_ComputerScience_isMember:54@\"," +
          "since : 2002," +
          "target : \"MySQL_ComputerScience_Person:4\"}]" +
        "->(v004)" +
        "(v006)-" +
          "[e016:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:6\"," +
          "key : \"MySQL_ComputerScience_isMember:63@\"," +
          "since : 2011," +
          "target : \"MySQL_ComputerScience_Person:3\"}]" +
        "->(v003)" +
        "(v007)-" +
          "[e017:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:7\"," +
          "key : \"MySQL_ComputerScience_isMember:72@\"," +
          "since : 2007," +
          "target : \"MySQL_ComputerScience_Person:2\"}]" +
        "->(v002)" +
        "(v008)-" +
          "[e018:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:8\"," +
          "key : \"MySQL_ComputerScience_isMember:81@\"," +
          "since : 2009," +
          "target : \"MySQL_ComputerScience_Person:1\"}]" +
        "->(v001)" +
        "(v009)-" +
          "[e019:isMember {graphs : \"MySQL_GraphDomain_GraphHead:2\"," +
          "source : \"MySQL_ComputerScience_Person:9\"," +
          "key : \"MySQL_ComputerScience_isMember:90@\"," +
          "since : 2011," +
          "target : \"MySQL_ComputerScience_Person:0\"}]" +
        "->(v000)" +
      "]" +
      "g1 {key : \"MySQL_ComputerScience_Person:1\"}[" +
        "(v100:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Axel\",key : \"MySQL_ComputerScience_Person:0\",age: 23})" +
        "(v101:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Maria\",key : \"MySQL_ComputerScience_Person:1\",age: 25})" +
        "(v102:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Lisa\",key : \"MySQL_ComputerScience_Person:2\",age: 23})" +
        "(v103:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Stefan\",key : \"MySQL_ComputerScience_Person:3\",age: 24})" +
        "(v104:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Mathias\",key : \"MySQL_ComputerScience_Person:4\",age: 24})" +
        "(v105:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Max\",key : \"MySQL_ComputerScience_Person:5\",age: 20})" +
        "(v106:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Lisa\",key : \"MySQL_ComputerScience_Person:6\",age: 18})" +
        "(v107:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Matti\",key : \"MySQL_ComputerScience_Person:7\",age: 24})" +
        "(v108:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Erik\",key : \"MySQL_ComputerScience_Person:8\",age: 24})" +
        "(v109:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"male\",name : \"Sven\",key : \"MySQL_ComputerScience_Person:9\",age: 23})" +
        "(v110:Person {graphs : \"MySQL_ComputerScience_Person:1%MySQL_GraphDomain_GraphHead:2\"," +
        "gender : \"female\",name : \"Bea\",key : \"MySQL_ComputerScience_Person:10\",age: 26})" +
      "]");

    collectAndAssertTrue(
      collection.equalsByGraphElementData(loader.getGraphCollectionByVariables("g0", "g1")));
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
