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

package org.gradoop.io.graphgen;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.io.graphgen.tuples.GraphGenEdge;
import org.gradoop.io.graphgen.tuples.GraphGenGraph;
import org.gradoop.io.graphgen.tuples.GraphGenGraphHead;
import org.gradoop.io.graphgen.tuples.GraphGenVertex;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class EPGMDatabaseGraphGenTest  extends GradoopFlinkTestBase {

  /**
   * Test method for
   * {@link GraphGenStringToCollection#getGraphCollection() getGraphCollection}
   */
  @Test
  public void testGetGraphCollection() {
    Collection<GraphGenGraph> graphCollection = new HashSet<>();
    GraphGenGraphHead graphHead = new GraphGenGraphHead();
    Collection<GraphGenVertex> vertices = new HashSet<>();
    Collection<GraphGenEdge> edges = new HashSet<>();

    //Graph 0
    graphHead.setId(0L);
    vertices.add(new GraphGenVertex(0, "00"));
    vertices.add(new GraphGenVertex(1, "11"));
    edges.add(new GraphGenEdge(0, 1, "01"));

    graphCollection.add(new GraphGenGraph(graphHead, vertices, edges));

    graphHead = new GraphGenGraphHead();
    vertices = new HashSet<>();
    edges = new HashSet<>();

    //Graph 1
    graphHead.setId(1L);
    vertices.add(new GraphGenVertex(2, "22"));
    vertices.add(new GraphGenVertex(3, "33"));
    vertices.add(new GraphGenVertex(4, "44"));
    edges.add(new GraphGenEdge(2, 3, "23"));
    edges.add(new GraphGenEdge(4, 3, "43"));

    graphCollection.add(new GraphGenGraph(graphHead, vertices, edges));

    String testGraphs = "t # 0\n" +
      "v 0 00\n" +
      "v 1 11\n" +
      "e 0 1 01\n" +
      "t # 1\n"+
      "v 2 22\n" +
      "v 3 33\n" +
      "v 4 44\n" +
      "e 2 3 23\n" +
      "e 4 3 43\n";
    GraphGenStringToCollection graphGenStringToCollection = new
      GraphGenStringToCollection();
    graphGenStringToCollection.setContent(testGraphs);

    assertEquals("test", graphGenStringToCollection.getGraphCollection(),
      graphCollection);
  }

  /**
   * Test method for
   * {@link org.gradoop.model.impl.EPGMDatabase#fromGraphGenFile(String, ExecutionEnvironment) fromgraphGenFile}
   * @throws Exception
   */
  @Test
  public void testFromGraphGenFile() throws Exception {
    String graphGenFile =
      EPGMDatabaseGraphGenTest.class.getResource("/data/graphgen/io_test.gg")
        .getFile();

    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> graphStore =
      EPGMDatabase.fromGraphGenFile(graphGenFile, getExecutionEnvironment());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> databaseGraph =
      graphStore.getDatabaseGraph();

    Collection<GraphHeadPojo> graphHeads = Lists.newArrayList();
    Collection<VertexPojo> vertices = Lists.newArrayList();
    Collection<EdgePojo> edges = Lists.newArrayList();

    graphStore.getCollection().getGraphHeads().output(new
      LocalCollectionOutputFormat<>(graphHeads));
    databaseGraph.getVertices().output(new LocalCollectionOutputFormat<>
      (vertices));
    databaseGraph.getEdges().output(new LocalCollectionOutputFormat<>(edges));

    getExecutionEnvironment().execute();

    assertEquals("Wrong graph count", 2, graphHeads.size());
    assertEquals("Wrong vertex count", 5, vertices.size());
    assertEquals("Wrong edge count", 3, edges.size());
  }
}
