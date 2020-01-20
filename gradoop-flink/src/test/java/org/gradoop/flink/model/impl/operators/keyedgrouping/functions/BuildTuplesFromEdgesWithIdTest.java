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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.LabelKeyFunction;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link BuildTuplesFromEdgesWithId} function.
 */
public class BuildTuplesFromEdgesWithIdTest extends GradoopFlinkTestBase {

  /**
   * Test the {@link BuildTuplesFromEdgesWithId#map(Edge)} functionality.
   *
   * @throws Exception when the function throws an exception
   */
  @Test
  public void testMap() throws Exception {
    GradoopId source = GradoopId.get();
    GradoopId target = GradoopId.get();
    Edge testEdge = getConfig().getLogicalGraphFactory().getEdgeFactory().createEdge(source, target);
    String testLabel = "a";
    String testAggKey = "key";
    PropertyValue testAggValue = PropertyValue.create(17L);
    testEdge.setLabel(testLabel);
    testEdge.setProperty(testAggKey, testAggValue);
    BuildTuplesFromEdgesWithId<Edge> function = new BuildTuplesFromEdgesWithId<>(
      singletonList(new LabelKeyFunction<>()), singletonList(new MaxProperty(testAggKey)));
    Tuple result = function.map(testEdge);
    assertEquals("Invalid result tuple size", 5, result.getArity());
    assertEquals(testEdge.getId(), result.getField(0));
    assertEquals(source, result.getField(1));
    assertEquals(target, result.getField(2));
    assertEquals(testLabel, result.getField(3));
    assertEquals(testAggValue, result.getField(4));
  }
}
