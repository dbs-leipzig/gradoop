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
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DefaultVertexCompareFunctionTest {

  @Test
  public void testCompare() {
    DefaultVertexCompareFunction cf = new DefaultVertexCompareFunction(10);

    LVertex v1 = new LVertex(null, new Vector(10, 10), -1, null, new Vector(5, 0));
    LVertex v2 = new LVertex(null, new Vector(10, 10), -1, null, new Vector(-5, 0));
    LVertex v3 = new LVertex(null, new Vector(10, 10), -1, null, new Vector(6, 0));
    Assert.assertEquals(1, cf.compare(v1, v1), 0.0000001);
    Assert.assertEquals(0, cf.compare(v1, v2), 0.0000001);
    Assert.assertTrue(cf.compare(v1, v3) > 0.5);

    LVertex v4 = new LVertex(null, new Vector(10, 20), -1, null, new Vector(5, 0));
    LVertex v5 = new LVertex(null, new Vector(10, 30), -1, null, new Vector(5, 0));
    LVertex v6 = new LVertex(null, new Vector(10, 15), -1, null, new Vector(5, 0));
    Assert.assertEquals(1, cf.compare(v1, v4), 0.0000001);
    Assert.assertEquals(1, cf.compare(v1, v6), 0.0000001);
    Assert.assertEquals(0, cf.compare(v1, v5), 0.0000001);

    LVertex v7 = new LVertex(null, new Vector(10, 10), -1,
      Arrays.asList(GradoopId.get(), GradoopId.get(), GradoopId.get()), new Vector(50, 0));
    Assert.assertEquals(1, cf.compare(v7, v7), 0.0000001);
    Assert.assertEquals(0, cf.compare(v7, v2), 0.0000001);
    Assert.assertTrue(cf.compare(v7, v3) > 0.5);
  }
}
