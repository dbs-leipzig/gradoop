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
package org.gradoop.flink.model.impl.operators.aggregation.functions.average;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.assertEquals;

/**
 * Test for the average property aggregate function.
 */
public class AveragePropertyTest extends GradoopFlinkTestBase {

  /**
   * Rule used to check for expected exception.
   */
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Test the average aggregation on a graph with some values.
   */
  @Test
  public void testWithGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(i1 {a: 1L, b: 2.4})-[e1{c: 3, d: 10.1}]->(i2 {a: 12.2, b: 12.3})-[e2{c: 6, d: 6.3}]->(i1)" +
      "(i3 {e: 44L})" +
      "]");
    LogicalGraph result = loader.getLogicalGraphByVariable("input").aggregate(
      new AverageVertexProperty("a"),
      new AverageVertexProperty("b", "b_average"),
      new AverageVertexProperty("e"),
      new AverageVertexProperty("f"),
      new AverageEdgeProperty("c"),
      new AverageEdgeProperty("d"));
    // We have to check the head instead of the full graph, to allow for double comparison with
    // possible rounding errors.
    Properties resultProperties = result.getGraphHead().collect().get(0).getProperties();
    assertEquals(6.6d, resultProperties.get("avg_a").getDouble(), 0.001d);
    assertEquals(7.35d, resultProperties.get("b_average").getDouble(), 0.001d);
    assertEquals(4.5d, resultProperties.get("avg_c").getDouble(), 0d);
    assertEquals(8.2d, resultProperties.get("avg_d").getDouble(), 0.001d);
    assertEquals(44d, resultProperties.get("avg_e").getDouble(), 0d);
    assertEquals(PropertyValue.NULL_VALUE, resultProperties.get("avg_f"));
  }

  /**
   * Test if the conversion to the internal aggregate value representation works as
   * expected for some values.
   */
  @Test
  public void testAsInternalAggregate() {
    assertEquals(create(Arrays.asList(create(12.34d), create(1L))),
      AverageProperty.asInternalAggregate(PropertyValue.create(12.34d)));
    assertEquals(create(Arrays.asList(create(1), create(1L))),
      AverageProperty.asInternalAggregate(PropertyValue.create(1)));
    assertEquals(create(Arrays.asList(create(2L), create(1L))),
      AverageProperty.asInternalAggregate(PropertyValue.create(2L)));
    // Check an unsupported value.
    expectedException.expect(IllegalArgumentException.class);
    AverageProperty.asInternalAggregate(create(""));
  }
}
