package org.gradoop.io.graphgen;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * Created by stephan on 18.05.16.
 */
public class EPGMDatabaseGraphGenTest  extends GradoopFlinkTestBase {

  @Test
  public void testGetGraphCollection() {
    Collection<Tuple3<
      Long,
      Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>>> graphCollection = new
      HashSet<>();
    Collection<Tuple2<Integer, String>> vertices = new HashSet<>();
    Collection<Tuple3<Integer, Integer, String>> edges = new HashSet<>();

    //Graph 0
    vertices.add(new Tuple2<Integer, String>(0, "00"));
    vertices.add(new Tuple2<Integer, String>(1, "11"));
    edges.add(new Tuple3<Integer, Integer, String>(0, 1, "01"));

    graphCollection.add(new Tuple3<Long, Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>>(0L, vertices, edges));

    vertices = new HashSet<>();
    edges = new HashSet<>();

    //Graph 1
    vertices.add(new Tuple2<Integer, String>(2, "22"));
    vertices.add(new Tuple2<Integer, String>(3, "33"));
    vertices.add(new Tuple2<Integer, String>(4, "44"));
    edges.add(new Tuple3<Integer, Integer, String>(2, 3, "23"));
    edges.add(new Tuple3<Integer, Integer, String>(4, 3, "43"));

    graphCollection.add(new Tuple3<Long, Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>>(1L, vertices, edges));

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
    GraphGenReader graphGenReader = new GraphGenReader(testGraphs);

    assertEquals("test", graphGenReader.getGraphCollection(), graphCollection);

  }
}
