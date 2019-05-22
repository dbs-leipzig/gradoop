/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

public class CrossEdgesTest extends GradoopFlinkTestBase {

  @Test
  public void intersectTest() {
    CrossEdges ce = new CrossEdges();
    Edge e1 = getDummyEdge(3, 4, 3, 2);
    Edge e2 = getDummyEdge(2, 3, 4, 3);
    Edge e3 = getDummyEdge(3, 2, 7, 4);
    Edge e4 = getDummyEdge(3, 2, 1, 1);
    Edge e5 = getDummyEdge(4, 4, 4, 2);
    Edge e6 = getDummyEdge(0, 0, 10, 10);
    Edge e7 = getDummyEdge(0, 10, 10, 0);
    Edge e8 = getDummyEdge(0, 0, 0, 0);
    Edge e9 = getDummyEdge(1, 1, 2, 1);
    Edge e10 = getDummyEdge(2, 1, 3, 1);
    Edge e11 = getDummyEdge(1, 1, 1, 2);
    Edge e12 = getDummyEdge(1, 2, 1, 3);
    Edge e13 = getDummyEdge(1, 2, 1, 4);

    Assert.assertTrue(ce.intersect(e1, e2));
    Assert.assertTrue(ce.intersect(e2, e1));
    Assert.assertFalse(ce.intersect(e1, e3));
    Assert.assertFalse(ce.intersect(e2, e3));
    Assert.assertFalse(ce.intersect(e1, e4));
    Assert.assertFalse(ce.intersect(e4, e1));
    Assert.assertFalse(ce.intersect(e1, e1));
    Assert.assertFalse(ce.intersect(e5, e1));
    Assert.assertTrue(ce.intersect(e6, e7));
    Assert.assertTrue(ce.intersect(e7, e6));
    Assert.assertFalse(ce.intersect(e8, e8));
    Assert.assertFalse(ce.intersect(e1, e8));
    Assert.assertFalse(ce.intersect(e9, e10));
    Assert.assertFalse(ce.intersect(e10, e9));
    Assert.assertFalse(ce.intersect(e11, e12));
    Assert.assertFalse(ce.intersect(e12, e13));
    Assert.assertFalse(ce.intersect(e13, e12));

  }

  private Edge getDummyEdge(int x1, int y1, int x2, int y2) {
    Edge e =
      new Edge(GradoopId.get(), "testedge", GradoopId.get(), GradoopId.get(), new Properties(),
        null);
    e.setProperty("source_x", x1);
    e.setProperty("source_y", y1);
    e.setProperty("target_x", x2);
    e.setProperty("target_y", y2);
    return e;
  }

  @Test
  public void crossingEdgesTest() throws Exception {

    // a special graph with a known number of edge-crossings
    String graph =
      "g1:graph[" + "(p1:Person {name: \"Bob\", age: 24, X: 57, Y: 0})-[:friendsWith]->" +
        "(p2:Person{name: \"Alice\", age: 30, X:316, Y: 0})-[:friendsWith]->(p1)" +
        "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27, X: 186, Y: 0})" +
        "-[:friendsWith]->(p2) " +
        "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40, X: 382, Y: 120})" +
        "-[:friendsWith]->(p3) " +
        "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33, X: 583, Y: 153})" +
        "-[:friendsWith]->(p4) " + "(c1:Company {name: \"Acme Corp\", X: 599, Y: 0}) " +
        "(c2:Company {name: \"Globex Inc.\", X: 0, Y: 0}) " + "(p2)-[:worksAt]->(c1) " +
        "(p4)-[:worksAt]->(c1) " + "(p5)-[:worksAt]->(c1) " + "(p1)-[:worksAt]->(c2) " +
        "(p3)-[:worksAt]->(c2) " + "] " + "g2:graph[" +
        "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37, X: 124, Y: 164})" +
        "-[:friendsWith]->(p4) " +
        "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23, X: 190, Y: 76})" +
        "-[:friendsWith]->(p6) " +
        "(p8:Person {name: \"Jil\", age: 32, X: 434, Y: 0})-[:friendsWith]->(p7)-[:friendsWith]->" +
        "(p8) " + "(p6)-[:worksAt]->(c2) " + "(p7)-[:worksAt]->(c2) " + "(p8)-[:worksAt]->(c1) " +
        "]";

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graph);

    LogicalGraph g = loader.getLogicalGraph();

    CrossEdges ce = new CrossEdges();
    DataSet<Tuple2<Integer, Double>> crossing = ce.execute(g);
    Tuple2<Integer, Double> results = crossing.collect().get(0);

    Assert.assertEquals(1, (int) results.f0);
    Assert.assertEquals(0.06666666, results.f1, 0.0001);
  }
}
