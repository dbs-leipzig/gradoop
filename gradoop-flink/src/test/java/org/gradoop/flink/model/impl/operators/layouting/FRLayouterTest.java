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
package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdMapper;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRCellIdSelector;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FRLayouterTest extends LayoutingAlgorithmTest {

  @Test
  public void testCellIdSelector() throws Exception {
    int resolution = 10;
    KeySelector<Vertex, Integer> selfselector =
      new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.SELF);
    FRCellIdMapper mapper = new FRCellIdMapper(10, 100, 100);

    Assert.assertEquals(0, (long) selfselector.getKey(mapper.map(getDummyVertex(0, 0))));
    Assert.assertEquals(99, (long) selfselector.getKey(mapper.map(getDummyVertex(99, 98))));
    Assert.assertEquals(90, (long) selfselector.getKey(mapper.map(getDummyVertex(0, 95))));

    KeySelector<Vertex, Integer> neighborslector =
      new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.RIGHT);
    Assert.assertEquals(01, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(36, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.LEFT);
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(34, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(98, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.UP);
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(25, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(89, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.DOWN);
    Assert.assertEquals(10, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(45, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.UPRIGHT);
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(26, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.UPLEFT);
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(24, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(88, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.DOWNLEFT);
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(44, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

    neighborslector = new FRCellIdSelector(resolution, FRCellIdSelector.NeighborType.DOWNRIGHT);
    Assert.assertEquals(11, (long) neighborslector.getKey(getDummyVertex(0)));
    Assert.assertEquals(46, (long) neighborslector.getKey(getDummyVertex(35)));
    Assert.assertEquals(-1, (long) neighborslector.getKey(getDummyVertex(99)));

  }

  @Test
  public void testRepulseJoinFunction() throws Exception {
    JoinFunction<Vertex, Vertex, Tuple3<GradoopId, Double, Double>> jf = new FRRepulsionFunction(1);
    Vertex v1 = getDummyVertex(1, 1);
    Vertex v2 = getDummyVertex(2, 3);
    Vertex v3 = getDummyVertex(7, 5);
    Vertex v4 = getDummyVertex(1, 1);

    Vector vec12 = Vector.fromForceTuple(jf.join(v1, v2));
    Vector vec13 = Vector.fromForceTuple(jf.join(v1, v3));
    Vector vec14 = Vector.fromForceTuple(jf.join(v1, v4));
    Vector vec11 = Vector.fromForceTuple(jf.join(v1, v1));

    Assert.assertTrue(vec12.getX() < 0 && vec12.getY() < 0);
    Assert.assertTrue(vec12.magnitude() > vec13.magnitude());
    Assert.assertTrue(vec14.magnitude() > 0);
    Assert.assertTrue(vec11.magnitude() == 0);
  }

  @Test
  public void testRepulseFlatMap() throws Exception {
    FRRepulsionFunction jf = new FRRepulsionFunction(1);
    Vertex v1 = getDummyVertex(1, 1);
    Vertex v2 = getDummyVertex(2, 3);

    Vector vec12join = Vector.fromForceTuple(jf.join(v1, v2));

    List<Tuple3<GradoopId, Double, Double>> collectorList = new ArrayList<>();
    ListCollector<Tuple3<GradoopId, Double, Double>> collector = new ListCollector<>(collectorList);
    jf.flatMap(new Tuple2<>(v1, v2), collector);

    Vector vec12 = Vector.fromForceTuple(collectorList.get(0));
    Vector vec21 = Vector.fromForceTuple(collectorList.get(1));

    Assert.assertEquals(vec12join, vec12);
    Assert.assertEquals(vec12, vec21.mul(-1));
  }


  private Vertex getDummyVertex(int cellid) {
    Vertex v = new Vertex(new GradoopId(), "testlabel", new Properties(), null);
    v.setProperty(FRLayouter.CELLID_PROPERTY, new Integer(cellid));
    return v;
  }

  private Vertex getDummyVertex(int x, int y) throws Exception {
    Vertex v = new Vertex(GradoopId.get(), "testlabel", new Properties(), null);
    Vector pos = new Vector(x, y);
    pos.setVertexPosition(v);
    return v;
  }

  @Override
  public LayoutingAlgorithm getLayouter(int w, int h) {
    return new FRLayouter(FRLayouter.calculateK(w, h, 10), 5, w, h, 4);
  }
}
