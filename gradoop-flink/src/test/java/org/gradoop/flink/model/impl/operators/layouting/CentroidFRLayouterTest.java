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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CentroidFRLayouterTest extends LayoutingAlgorithmTest {

  @Override
  public LayoutingAlgorithm getLayouter(int w, int h) {
    return new CentroidFRLayouter(5, 10);
  }

  @Test
  public void testAverageCalculation() throws Exception {
    DataSet<LVertex> tv = getExecutionEnvironment()
      .fromElements(new LVertex(GradoopId.get(), new Vector(10, 10)),
        new LVertex(GradoopId.get(), new Vector(20, 20)),
        new LVertex(GradoopId.get(), new Vector(30, 30)));
    List<Vector> avg = CentroidFRLayouter.averagePosition(tv).collect();
    Assert.assertEquals(1, avg.size());
    Assert.assertEquals(new Vector(20, 20), avg.get(0));
  }

  @Test
  public void testRepulsionForceCalculator() {
    List<CentroidFRLayouter.Centroid> centroids = new ArrayList<>();
    centroids.add(new CentroidFRLayouter.Centroid(new Vector(3, 3), 0));
    centroids.add(new CentroidFRLayouter.Centroid(new Vector(7, 7), 0));
    List<Vector> center = new ArrayList<>();
    center.add(new Vector(5, 5));

    FRRepulsionFunction rf = new FRRepulsionFunction(10);
    CentroidFRLayouter.RepulsionForceMapper calc =
      new CentroidFRLayouter.RepulsionForceMapper(rf);
    // manually set centroids and center, as we do not call open() like Flink would
    calc.centroids = centroids;
    calc.center = center;

    LVertex vertex = new LVertex(GradoopId.get(), new Vector(1, 1));

    Force f = calc.map(vertex).copy();

    Assert.assertEquals(f.getId(), vertex.getId());
    Assert.assertTrue(f.getValue().getX() < 0 && f.getValue().getY() < 0);

    centroids.add(new CentroidFRLayouter.Centroid(new Vector(-1, -1), 0));
    Force f2 = calc.map(vertex).copy();

    Assert.assertTrue(f2.getValue().magnitude() < f.getValue().magnitude());
  }

  @Test
  public void testCentroidUpdater() {
    List<CentroidFRLayouter.Centroid> centroids = new ArrayList<>();
    centroids.add(new CentroidFRLayouter.Centroid(new Vector(3, 3), 0));
    centroids.add(new CentroidFRLayouter.Centroid(new Vector(7, 7), 0));
    CentroidFRLayouter.CentroidUpdater upd =
      new CentroidFRLayouter.CentroidUpdater(1000);
      upd.centroids = centroids;

    //test map() (map vertex to closest centroid)
    LVertex vertex = new LVertex(GradoopId.get(), new Vector(4, 4));
    LVertex vertex2 = new LVertex(GradoopId.get(), new Vector(100, 5));
    LVertex vertex3 = new LVertex(GradoopId.get(), new Vector(0, 0));
    Assert.assertEquals(centroids.get(0).getId(), upd.map(vertex).getId());
    Assert.assertEquals(centroids.get(1).getId(), upd.map(vertex2).getId());
    Assert.assertEquals(centroids.get(0).getId(), upd.map(vertex3).getId());

    // test reduce() (calculate new centroid position from all assigned vertices)
    List<Force> forces = new ArrayList<>();
    forces.add(new Force(null, new Vector(10, 10)));
    forces.add(new Force(null, new Vector(20, 20)));
    forces.add(new Force(null, new Vector(30, 30)));
    List<CentroidFRLayouter.Centroid> collectorList = new ArrayList<>();
    ListCollector<CentroidFRLayouter.Centroid> collector = new ListCollector<>(collectorList);

    upd.calculateNewCentroidPosition(forces, collector);
    Assert.assertEquals(1, collectorList.size());
    Assert.assertEquals(new Vector(20, 20), collectorList.get(0).getPosition());
    Assert.assertEquals(3, collectorList.get(0).getCount());

    // test flatMap() (filters and splits centroids based on their vertex-count
    CentroidFRLayouter.Centroid toFew = new CentroidFRLayouter.Centroid(new Vector(), 2);
    CentroidFRLayouter.Centroid toMany = new CentroidFRLayouter.Centroid(new Vector(), 100);
    CentroidFRLayouter.Centroid ok = new CentroidFRLayouter.Centroid(new Vector(), 30);
    collectorList.clear();

    upd.removeOrSplitCentroids(toFew, collector);
    Assert.assertEquals(0, collectorList.size());

    upd.removeOrSplitCentroids(toMany, collector);
    Assert.assertEquals(2, collectorList.size());
    Assert.assertEquals(50, collectorList.get(0).getCount());
    Assert.assertEquals(50, collectorList.get(1).getCount());
    collectorList.clear();

    upd.removeOrSplitCentroids(ok, collector);
    Assert.assertEquals(1, collectorList.size());
    Assert.assertEquals(30, collectorList.get(0).getCount());
    Assert.assertEquals(ok.getId(), collectorList.get(0).getId());
  }
}
