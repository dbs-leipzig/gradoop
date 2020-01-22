/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.layouting.util.Centroid;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CentroidRepulsionForceMapperTest extends GradoopFlinkTestBase {

  @Test
  public void testRepulsionForceCalculator() {
    List<Centroid> centroids = new ArrayList<>();
    centroids.add(new Centroid(new Vector(3, 3), 0));
    centroids.add(new Centroid(new Vector(7, 7), 0));
    List<Vector> center = new ArrayList<>();
    center.add(new Vector(5, 5));

    FRRepulsionFunction rf = new FRRepulsionFunction(10);
    CentroidRepulsionForceMapper calc =
      new CentroidRepulsionForceMapper(rf);
    // manually set centroids and center, as we do not call open() like Flink would
    calc.centroids = centroids;
    calc.center = center;

    LVertex vertex = new LVertex(GradoopId.get(), new Vector(1, 1));

    Force f = calc.map(vertex).copy();

    Assert.assertEquals(f.getId(), vertex.getId());
    Assert.assertTrue(f.getValue().getX() < 0 && f.getValue().getY() < 0);

    centroids.add(new Centroid(new Vector(-1, -1), 0));
    Force f2 = calc.map(vertex).copy();

    Assert.assertTrue(f2.getValue().magnitude() < f.getValue().magnitude());
  }
}
