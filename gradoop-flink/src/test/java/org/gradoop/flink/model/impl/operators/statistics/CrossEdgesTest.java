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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CrossEdgesTest extends GradoopFlinkTestBase {

  // a special graph with a known number of edge-crossings
  static final String graph =
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

  @Test
  public void intersectTest() {
    CrossEdges.Line e1 = new CrossEdges.Line(3, 4, 3, 2);
    CrossEdges.Line e2 = new CrossEdges.Line(2, 3, 4, 3);
    CrossEdges.Line e3 = new CrossEdges.Line(3, 2, 7, 4);
    CrossEdges.Line e4 = new CrossEdges.Line(3, 2, 1, 1);
    CrossEdges.Line e5 = new CrossEdges.Line(4, 4, 4, 2);
    CrossEdges.Line e6 = new CrossEdges.Line(0, 0, 10, 10);
    CrossEdges.Line e7 = new CrossEdges.Line(0, 10, 10, 0);
    CrossEdges.Line e8 = new CrossEdges.Line(0, 0, 0, 0);
    CrossEdges.Line e9 = new CrossEdges.Line(1, 1, 2, 1);
    CrossEdges.Line e10 = new CrossEdges.Line(2, 1, 3, 1);
    CrossEdges.Line e11 = new CrossEdges.Line(1, 1, 1, 2);
    CrossEdges.Line e12 = new CrossEdges.Line(1, 2, 1, 3);
    CrossEdges.Line e13 = new CrossEdges.Line(1, 2, 1, 4);

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
  public void pointOnLineTest() {
    CrossEdges.Line l = new CrossEdges.Line(1, 2, 10, 20);
    Vector p1 = new Vector(1, 2);
    Vector p2 = new Vector(10, 20);
    Vector p3 = new Vector(2, 4);
    Vector p4 = new Vector(4, 8);
    Vector p5 = new Vector(5, 7);
    Vector p6 = new Vector(15, 30);

    Assert.assertFalse(l.isPointOnLine(p1));
    Assert.assertFalse(l.isPointOnLine(p2));
    Assert.assertFalse(l.isPointOnLine(p5));
    Assert.assertFalse(l.isPointOnLine(p6));

    Assert.assertTrue(l.isPointOnLine(p3));
    Assert.assertTrue(l.isPointOnLine(p4));
  }

  @Test
  public void parallelTest() {
    CrossEdges.Line l1 = new CrossEdges.Line(1, 2, 10, 20);
    CrossEdges.Line l2 = new CrossEdges.Line(2, 4, 9, 18);
    CrossEdges.Line l3 = new CrossEdges.Line(1, 5, 15, 23);

    Assert.assertTrue(l1.isParallel(l1));
    Assert.assertTrue(l1.isParallel(l2));
    Assert.assertTrue(l2.isParallel(l1));

    Assert.assertFalse(l1.isParallel(l3));
    Assert.assertFalse(l3.isParallel(l1));

    Assert.assertFalse(l2.isParallel(l3));
    Assert.assertFalse(l3.isParallel(l2));
  }

  @Test
  public void overlapTest() {
    CrossEdges.Line l1 = new CrossEdges.Line(1, 2, 10, 20);
    CrossEdges.Line l2 = new CrossEdges.Line(2, 4, 9, 18);
    CrossEdges.Line l3 = new CrossEdges.Line(1, 2, 9, 18);
    CrossEdges.Line l4 = new CrossEdges.Line(2, 4, 9, 18);
    CrossEdges.Line l5 = new CrossEdges.Line(1, 2, 15, 18);
    CrossEdges.Line l6 = new CrossEdges.Line(1, 5, 15, 23);

    Assert.assertTrue(l1.overlaps(l1));
    Assert.assertTrue(l1.overlaps(l2));
    Assert.assertTrue(l2.overlaps(l1));
    Assert.assertTrue(l1.overlaps(l3));
    Assert.assertTrue(l3.overlaps(l1));
    Assert.assertTrue(l1.overlaps(l4));
    Assert.assertTrue(l4.overlaps(l1));

    Assert.assertFalse(l1.overlaps(l5));
    Assert.assertFalse(l5.overlaps(l1));
    Assert.assertFalse(l1.overlaps(l6));
    Assert.assertFalse(l6.overlaps(l1));
  }

  @Test
  public void getNextGridCrossingTest() {
    CrossEdges.LinePartitioner lp = new CrossEdges.LinePartitioner(10);
    //exactly diagonal
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 20d),
      lp.firstGridIntersection(new CrossEdges.Line(15, 15, 25, 25)));

    //exactly diagonal to corner
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 20d),
      lp.firstGridIntersection(new CrossEdges.Line(15, 15, 20, 20)));

    //exactly diagonal from corner
    Assert.assertNull(lp.firstGridIntersection(new CrossEdges.Line(20, 20, 15, 15)));

    //diagonal
    Tuple2<Double, Double> cross = lp.firstGridIntersection(new CrossEdges.Line(15, 15, 25, 20));
    Assert.assertEquals(20, cross.f0, 0.00001);
    Assert.assertEquals(17.5, cross.f1, 0.00001);

    //diagonal inverted
    Tuple2<Double, Double> cross2 = lp.firstGridIntersection(new CrossEdges.Line(25, 20, 15, 15));
    Assert.assertEquals(cross, cross2);

    //horizontal
    Tuple2<Double, Double> cross3 = lp.firstGridIntersection(new CrossEdges.Line(15, 15, 40, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(20d, 15d), cross3);

    //horizontal inverted
    Tuple2<Double, Double> cross4 = lp.firstGridIntersection(new CrossEdges.Line(40, 15, 15, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(30d, 15d), cross4);

    //vertical
    Tuple2<Double, Double> cross5 = lp.firstGridIntersection(new CrossEdges.Line(15, 15, 15, 40));
    Assert.assertEquals(new Tuple2<Double, Double>(15d, 20d), cross5);

    //vertical inverted
    Tuple2<Double, Double> cross6 = lp.firstGridIntersection(new CrossEdges.Line(15, 40, 15, 15));
    Assert.assertEquals(new Tuple2<Double, Double>(15d, 30d), cross6);

    //completely inside
    Assert.assertNull(lp.firstGridIntersection(new CrossEdges.Line(15, 13, 16, 17)));

    //completely inside inverted
    Assert.assertNull(lp.firstGridIntersection(new CrossEdges.Line(15, 13, 16, 17)));

  }

  @Test
  public void lineDivisionTest() {
    CrossEdges.LinePartitioner lp = new CrossEdges.LinePartitioner(10);
    CrossEdges.Line l = new CrossEdges.Line(10, 11, 100, 90);
    List<CrossEdges.Line> parts = lp.subdivideByGrid(l);

    Assert.assertEquals(parts.get(0).getStart().getX(), l.getStart().getX(), 0.00001);
    Assert.assertEquals(parts.get(0).getStart().getY(), l.getStart().getY(), 0.00001);
    for (int i = 0; i < parts.size(); i++) {
      Assert.assertEquals(l.getId(), parts.get(i).getId());
      if (i > 0) {
        Assert
          .assertEquals(parts.get(i).getStart().getX(), parts.get(i - 1).getEnd().getX(), 0.00001);
        Assert
          .assertEquals(parts.get(i).getStart().getY(), parts.get(i - 1).getEnd().getY(), 0.00001);
      }
    }
    Assert.assertEquals(parts.get(parts.size() - 1).getEnd().getX(), l.getEnd().getX(), 0.00001);
    Assert.assertEquals(parts.get(parts.size() - 1).getEnd().getY(), l.getEnd().getY(), 0.00001);
    Assert.assertTrue(parts.size() == 16);
  }

  private EPGMEdge getDummyEdge(int x1, int y1, int x2, int y2) {
    EPGMEdge e =
      new EPGMEdge(GradoopId.get(), "testedge", GradoopId.get(), GradoopId.get(), new Properties(),
        null);
    e.setProperty("source_x", x1);
    e.setProperty("source_y", y1);
    e.setProperty("target_x", x2);
    e.setProperty("target_y", y2);
    return e;
  }

  @Test
  public void crossingEdgesTest() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graph);

    LogicalGraph g = loader.getLogicalGraph();

    CrossEdges ce = new CrossEdges(10);
    DataSet<Tuple2<Long, Double>> crossing = ce.execute(g);
    Tuple2<Long, Double> results = crossing.collect().get(0);

    Assert.assertEquals(1, (long) results.f0);
    Assert.assertEquals(0.06666666, results.f1, 0.0001);
  }

  @Test
  public void crossingEdgesUnoptimizedTest() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graph);

    LogicalGraph g = loader.getLogicalGraph();

    CrossEdges ce = new CrossEdges(CrossEdges.DISABLE_OPTIMIZATION);
    DataSet<Tuple2<Long, Double>> crossing = ce.execute(g);
    Tuple2<Long, Double> results = crossing.collect().get(0);

    Assert.assertEquals(1, (long) results.f0);
    Assert.assertEquals(0.06666666, results.f1, 0.0001);
  }

  @Test
  public void crossingEdgesLocalTest() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
    loader.initDatabaseFromString(graph);

    LogicalGraph g = loader.getLogicalGraph();

    CrossEdges ce = new CrossEdges(10);
    Tuple2<Integer, Double> results = ce.executeLocally(g);


    Assert.assertEquals(1, (int) results.f0);
    Assert.assertEquals(0.06666666, results.f1, 0.0001);
  }
}
