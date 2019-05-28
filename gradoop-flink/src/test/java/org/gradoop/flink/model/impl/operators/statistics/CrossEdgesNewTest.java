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

import java.util.List;

public class CrossEdgesNewTest extends GradoopFlinkTestBase {

  @Test
  public void intersectTest() {
    CrossEdgesNew.Line e1 = new CrossEdgesNew.Line(3, 4, 3, 2);
    CrossEdgesNew.Line e2 = new CrossEdgesNew.Line(2, 3, 4, 3);
    CrossEdgesNew.Line e3 = new CrossEdgesNew.Line(3, 2, 7, 4);
    CrossEdgesNew.Line e4 = new CrossEdgesNew.Line(3, 2, 1, 1);
    CrossEdgesNew.Line e5 = new CrossEdgesNew.Line(4, 4, 4, 2);
    CrossEdgesNew.Line e6 = new CrossEdgesNew.Line(0, 0, 10, 10);
    CrossEdgesNew.Line e7 = new CrossEdgesNew.Line(0, 10, 10, 0);
    CrossEdgesNew.Line e8 = new CrossEdgesNew.Line(0, 0, 0, 0);
    CrossEdgesNew.Line e9 = new CrossEdgesNew.Line(1, 1, 2, 1);
    CrossEdgesNew.Line e10 = new CrossEdgesNew.Line(2, 1, 3, 1);
    CrossEdgesNew.Line e11 = new CrossEdgesNew.Line(1, 1, 1, 2);
    CrossEdgesNew.Line e12 = new CrossEdgesNew.Line(1, 2, 1, 3);
    CrossEdgesNew.Line e13 = new CrossEdgesNew.Line(1, 2, 1, 4);

    Assert.assertTrue(e1.intersects(e2));
    Assert.assertTrue(e2.intersects(e1));
    Assert.assertFalse(e1.intersects(e3));
    Assert.assertFalse(e2.intersects(e3));
    Assert.assertFalse(e1.intersects(e4));
    Assert.assertFalse(e4.intersects(e1));
    Assert.assertFalse(e1.intersects(e1));
    Assert.assertFalse(e5.intersects(e1));
    Assert.assertTrue(e6.intersects(e7));
    Assert.assertTrue(e7.intersects(e6));
    Assert.assertFalse(e8.intersects(e8));
    Assert.assertFalse(e1.intersects(e8));
    Assert.assertFalse(e9.intersects(e10));
    Assert.assertFalse(e10.intersects(e9));
    Assert.assertFalse(e11.intersects(e12));
    Assert.assertFalse(e12.intersects(e13));
    Assert.assertFalse(e13.intersects(e12));
  }

  @Test
  public void getNextGridCrossingTest() {
    CrossEdgesNew.LinePartitioner lp = new CrossEdgesNew.LinePartitioner(10);
    //exactly diagonal
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 20d),
      lp.firstGridIntersection(new CrossEdgesNew.Line(15, 15, 25, 25)));

    //exactly diagonal to corner
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 20d),
      lp.firstGridIntersection(new CrossEdgesNew.Line(15, 15, 20, 20)));

    //exactly diagonal from corner
    Assert.assertNull(lp.firstGridIntersection(new CrossEdgesNew.Line(20, 20, 15, 15)));

    //diagonal
    Tuple2<Double, Double> cross = lp.firstGridIntersection(new CrossEdgesNew.Line(15, 15, 25, 20));
    Assert.assertEquals(20, cross.f0, 0.00001);
    Assert.assertEquals(17.5, cross.f1, 0.00001);

    //diagonal inverted
    Tuple2<Double, Double> cross2 =
      lp.firstGridIntersection(new CrossEdgesNew.Line(25, 20, 15, 15));
    Assert.assertEquals(cross, cross2);

    //horizontal
    Tuple2<Double, Double> cross3 =
      lp.firstGridIntersection(new CrossEdgesNew.Line(15, 15, 40, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 15d), cross3);

    //horizontal inverted
    Tuple2<Double, Double> cross4 =
      lp.firstGridIntersection(new CrossEdgesNew.Line(40, 15, 15, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(30d, 15d), cross4);

    //vertical
    Tuple2<Double, Double> cross5 =
      lp.firstGridIntersection(new CrossEdgesNew.Line(15, 15, 15, 40));
    Assert.assertEquals(new Tuple2<Double, Double>(15d, 20d), cross5);

    //vertical inverted
    Tuple2<Double, Double> cross6 =
      lp.firstGridIntersection(new CrossEdgesNew.Line(15, 40, 15, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(15d, 30d), cross6);

    //completely inside
    Assert.assertNull(lp.firstGridIntersection(new CrossEdgesNew.Line(15, 13, 16, 17)));

    //completely inside inverted
    Assert.assertNull(lp.firstGridIntersection(new CrossEdgesNew.Line(15, 13, 16, 17)));

  }

  @Test
  public void lineDivisionTest() {
    CrossEdgesNew.LinePartitioner lp = new CrossEdgesNew.LinePartitioner(10);
    CrossEdgesNew.Line l = new CrossEdgesNew.Line(10, 11, 100, 90);
    List<CrossEdgesNew.Line> parts = lp.subdivideByGrid(l);

    Assert.assertEquals(parts.get(0).getStartX(), l.getStartX(), 0.00001);
    Assert.assertEquals(parts.get(0).getStartY(), l.getStartY(), 0.00001);
    for (int i = 0; i < parts.size(); i++) {
      Assert.assertEquals(l.getId(), parts.get(i).getId());
      if (i > 0) {
        Assert.assertEquals(parts.get(i).getStartX(), parts.get(i - 1).getEndX(), 0.00001);
        Assert.assertEquals(parts.get(i).getStartY(), parts.get(i - 1).getEndY(), 0.00001);
      }
    }
    Assert.assertEquals(parts.get(parts.size() - 1).getEndX(), l.getEndX(), 0.00001);
    Assert.assertEquals(parts.get(parts.size() - 1).getEndY(), l.getEndY(), 0.00001);
    Assert.assertTrue(parts.size() == 16);
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

    CrossEdgesNew ce = new CrossEdgesNew(10);
    DataSet<Tuple2<Integer, Double>> crossing = ce.execute(g);
    Tuple2<Integer, Double> results = crossing.collect().get(0);

    Assert.assertEquals(1, (int) results.f0);
    Assert.assertEquals(0.06666666, results.f1, 0.0001);
  }
}
