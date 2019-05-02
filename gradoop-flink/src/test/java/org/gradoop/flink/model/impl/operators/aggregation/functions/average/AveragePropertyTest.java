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

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for the average property aggregate function.
 */
public class AveragePropertyTest extends GradoopFlinkTestBase {

  /**
   * Test the average aggregation on a graph using the graph grouping operator.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithGraphGrouping() throws Exception {
    FlinkAsciiGraphLoader loader  = getLoaderFromString("input[" +
      "(:A {a: 1L, b: -10.1})(:A {a: 4L, b: -9L})(:B {a: -100.12, b: 12.1})(:B {a: -70.1, b:1L})" +
      "(:A)(:B)" +
      "]");
    LogicalGraph result = loader.getLogicalGraphByVariable("input").callForGraph(
      new Grouping.GroupingBuilder()
      .addVertexGroupingKey(Grouping.LABEL_SYMBOL)
      .addVertexAggregateFunction(new AverageVertexProperty("a"))
      .addVertexAggregateFunction(new AverageVertexProperty("b"))
      .setStrategy(GroupingStrategy.GROUP_COMBINE).build());
    // We have to collect the graph here, since results are doubles which can not be checked using
    // equals.
    List<Vertex> vertices = new ArrayList<>();
    List<Edge> edges = new ArrayList<>();
    result.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    result.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    getExecutionEnvironment().execute();
    assertEquals(0, edges.size());
    assertEquals(2, vertices.size());
    for (Vertex vertex : vertices) {
      PropertyValue valueA = vertex.getPropertyValue("avg_a");
      PropertyValue valueB = vertex.getPropertyValue("avg_b");
      assertNotNull("Aggregate property a was not set.", valueA);
      assertNotNull("Aggregate property b was not set.", valueB);
      assertTrue("Aggregate property a is not a double.", valueA.isDouble());
      assertTrue("Aggregate proeprty b is not a double.", valueB.isDouble());
      double aggregateA = valueA.getDouble();
      double aggregateB = valueB.getDouble();
      switch (vertex.getLabel()) {
      case "A":
        assertEquals(2.5d, aggregateA, 0.000001);
        assertEquals(-9.55, aggregateB, 0.000001);
        break;
      case "B":
        assertEquals(-85.11d, aggregateA, 0.000001);
        assertEquals(6.55d, aggregateB, 0.000001);
        break;
      default:
        fail("Unexpected label.");
      }
    }
  }

  /**
   * Test the average aggregation on a graph with some values.
   */
  @Test
  public void testWithLogicalGraph() throws Exception {
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
}
