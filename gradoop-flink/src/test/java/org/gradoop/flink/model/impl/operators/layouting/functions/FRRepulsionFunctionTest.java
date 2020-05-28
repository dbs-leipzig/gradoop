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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.layouting.functions.Util.generateSubVertices;
import static org.gradoop.flink.model.impl.operators.layouting.functions.Util.getDummyVertex;

public class FRRepulsionFunctionTest {

  @Test
  public void testRepulseJoinFunction() throws Exception {
    JoinFunction<LVertex, LVertex, Force> jf = new FRRepulsionFunction(1, 20);
    LVertex v1 = getDummyVertex(1, 1);
    LVertex v2 = getDummyVertex(2, 3);
    LVertex v3 = getDummyVertex(7, 5);
    LVertex v4 = getDummyVertex(1, 1);
    LVertex v5 = getDummyVertex(30, 30);

    Vector vec12 = jf.join(v1, v2).getValue().copy();
    Vector vec13 = jf.join(v1, v3).getValue().copy();
    Vector vec14 = jf.join(v1, v4).getValue().copy();
    Vector vec11 = jf.join(v1, v1).getValue().copy();
    Vector vec15 = jf.join(v1, v5).getValue().copy();

    Assert.assertTrue(vec12.getX() < 0 && vec12.getY() < 0);
    Assert.assertTrue(vec12.magnitude() > vec13.magnitude());
    Assert.assertTrue(vec14.magnitude() > 0);
    Assert.assertTrue(vec11.magnitude() == 0);
    Assert.assertTrue(vec15.magnitude() == 0);
  }

  @Test
  public void testRepulseFlatJoin() throws Exception {
    FRRepulsionFunction jf = new FRRepulsionFunction(1);
    LVertex v1 = getDummyVertex(1, 1);
    LVertex v2 = getDummyVertex(2, 3);
    Vector vec12join = jf.join(v1, v2).getValue().copy();

    v1 = getDummyVertex(1, 1);
    v2 = getDummyVertex(2, 3);
    List<Force> collectorList = new ArrayList<>();
    ListCollector<Force> collector = new ListCollector<>(collectorList);
    jf.join(v1, v2, collector);

    Vector vec12 = collectorList.get(0).getValue().copy();
    Vector vec21 = collectorList.get(1).getValue().copy();

    v1 = getDummyVertex(1, 1);
    v2 = getDummyVertex(2, 3);
    v1.setSubVertices(generateSubVertices(1));
    v2.setSubVertices(generateSubVertices(2));
    Vector vec12joinf6 = jf.join(v1, v2).getValue().copy();

    Assert.assertEquals(vec12join, vec12);
    Assert.assertEquals(vec12, vec21.mul(-1));
    Assert.assertNotEquals(collectorList.get(0).getId(), collectorList.get(1).getId());
    Assert.assertEquals(vec12join.mul(6), vec12joinf6);
  }
}
