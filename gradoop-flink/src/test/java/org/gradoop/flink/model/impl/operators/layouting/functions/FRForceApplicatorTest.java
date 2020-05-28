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
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

public class FRForceApplicatorTest {
  @Test
  public void testForceApplicator() {
    FRForceApplicator fa = new FRForceApplicator(1000, 1000, 10, 25);
    Assert.assertEquals(707.1, fa.speedForIteration(0), 0.1);
    Assert.assertEquals(537.96, fa.speedForIteration(1), 0.1);
    Assert.assertEquals(1.0, fa.speedForIteration(24), 0.1);

    Vector pos = new Vector(950, 0);
    LVertex v = new LVertex();
    v.setPosition(pos);

    Force force = new Force(null, new Vector(0, 300));

    fa.apply(v, force, 200);
    Assert.assertEquals(pos, new Vector(950, 200));

    Force force2 = new Force(null, new Vector(1000, 1000));
    fa.apply(v, force2, 10000);
    Assert.assertEquals(pos, new Vector(999, 999));

    v.addSubVertex(GradoopId.get());
    v.setPosition(new Vector(100, 100));
    Force force3 = new Force(null, new Vector(50, 50));
    fa.apply(v, force3, 10000);
    Assert.assertEquals(new Vector(125, 125), v.getPosition());
    Assert.assertEquals(new Vector(50, 50), v.getForce());
  }
}
