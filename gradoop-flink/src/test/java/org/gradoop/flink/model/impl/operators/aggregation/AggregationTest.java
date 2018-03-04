/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.runtime.client.JobExecutionException;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.storage.exceptions.UnsupportedTypeException;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends GradoopFlinkTestBase {
  
  static final String EDGE_PROPERTY = "ep";
  static final String VERTEX_PROPERTY = "vp";

  @Test
  public void testSingleGraphVertexAndEdgeMin() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp : 0.5f})-[:ea{ep : 2}]->(:Vb{vp : 3.1f})" +
          "(:Vc{vp : 0.33f})-[:eb]->(:Vd{vp : 0.0f})" +
          "]"
      )
      .getLogicalGraphByVariable("org");

    MinVertexProperty minVertexProperty =
      new MinVertexProperty(VERTEX_PROPERTY);

    MinEdgeProperty minEdgeProperty =
      new MinEdgeProperty(EDGE_PROPERTY);

    graph = graph
      .aggregate(minVertexProperty)
      .aggregate(minEdgeProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge minimum not set",
      graphHead.hasProperty(minEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex minimum not set",
      graphHead.hasProperty(minVertexProperty.getAggregatePropertyKey()));
    assertEquals(
      2,
      graphHead.getPropertyValue(
        minEdgeProperty.getAggregatePropertyKey()).getInt());
    assertEquals(
      0.0f,
      graphHead.getPropertyValue(
        minVertexProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
  }

  @Test
  public void testSingleGraphVertexAndEdgeMax() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp : 0.5f})-[:ea{ep : 2}]->(:Vb{vp : 3.1f})" +
          "(:Vc{vp : 0.33f})-[:eb]->(:Vd{vp : 0.0f})" +
          "]"
      )
      .getLogicalGraphByVariable("org");

    MaxVertexProperty maxVertexProperty =
      new MaxVertexProperty(VERTEX_PROPERTY);

    MaxEdgeProperty maxEdgeProperty =
      new MaxEdgeProperty(EDGE_PROPERTY);

    graph = graph
      .aggregate(maxVertexProperty)
      .aggregate(maxEdgeProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("vertex maximum not set",
      graphHead.hasProperty(maxEdgeProperty.getAggregatePropertyKey()));

    assertEquals(
      3.1f,
      graphHead.getPropertyValue(
        maxVertexProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);

    assertTrue("edge maximum not set",
      graphHead.hasProperty(maxVertexProperty.getAggregatePropertyKey()));

    assertEquals(
      2,
      graphHead.getPropertyValue(
        maxEdgeProperty.getAggregatePropertyKey()).getInt());
  }

  @Test
  public void testSingleGraphVertexAndEdgeSum() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp : 0.5f})-[:ea{ep : 2}]->(:Vb{vp : 3.1f})" +
          "(:Vc{vp : 0.33f})-[:eb]->(:Vd{vp : 0.0f})" +
          "]"
      ).getLogicalGraphByVariable("org");


    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(VERTEX_PROPERTY);

    SumEdgeProperty sumEdgeProperty =
      new SumEdgeProperty(EDGE_PROPERTY);

    graph = graph
      .aggregate(sumVertexProperty)
      .aggregate(sumEdgeProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set",
      graphHead.hasProperty(sumEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex sum not set",
      graphHead.hasProperty(sumVertexProperty.getAggregatePropertyKey()));
    assertEquals(
      2,
      graphHead.getPropertyValue(
        sumEdgeProperty.getAggregatePropertyKey()).getInt());
    assertEquals(
      3.93f,
      graphHead.getPropertyValue(
        sumVertexProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
  }

  @Test
  public void testGraphUnsupportedPropertyValueType() throws Exception{
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g[({a : 0})-[{b : 0.0}]->({a : true})-[{b : \"\"}]->({})]");

    LogicalGraph graph = loader.getLogicalGraphByVariable("g");

    try {
      graph
      .aggregate(new SumVertexProperty("a"))
      .aggregate(new SumEdgeProperty("b"))
      .getGraphHead().print();
    } catch (Exception e) {
      assertTrue(
        e instanceof JobExecutionException &&
          e.getCause() instanceof UnsupportedTypeException);
    }
  }

  @Test
  public void testSumWithEmptyProperties() throws Exception {
    LogicalGraph graph = getLoaderFromString(
        "org:Ga[(:Va)-[:ea]->(:Vb)]"
      )
      .getLogicalGraphByVariable("org");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(VERTEX_PROPERTY);

    SumEdgeProperty sumEdgeProperty =
      new SumEdgeProperty(EDGE_PROPERTY);

    graph = graph
      .aggregate(sumVertexProperty)
      .aggregate(sumEdgeProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set", graphHead.hasProperty(
      sumEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex sum not set", graphHead.hasProperty(
      sumVertexProperty.getAggregatePropertyKey()));

    assertEquals(PropertyValue.NULL_VALUE, graphHead.getPropertyValue(
        sumEdgeProperty.getAggregatePropertyKey()));
    assertEquals(PropertyValue.NULL_VALUE, graphHead.getPropertyValue(
        sumVertexProperty.getAggregatePropertyKey()));
  }

  @Test
  public void testSingleGraphVertexAndEdgeCount() throws Exception {
    LogicalGraph graph = getLoaderFromString("[()-->()<--()]")
      .getDatabase().getDatabaseGraph();

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();

    graph = graph
      .aggregate(vertexCount)
      .aggregate(edgeCount);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("vertex count not set",
      graphHead.hasProperty(vertexCount.getAggregatePropertyKey()));
    assertTrue("edge count not set",
      graphHead.hasProperty(edgeCount.getAggregatePropertyKey()));

    assertCounts(graphHead, 3L, 2L);
  }

  void assertCounts(EPGMGraphHead graphHead, long expectedVertexCount, long expectedEdgeCount) {

    assertEquals("wrong vertex count", expectedVertexCount,
      graphHead.getPropertyValue(
        new VertexCount().getAggregatePropertyKey()).getLong());
    assertEquals("wrong edge count", expectedEdgeCount,
      graphHead.getPropertyValue(
        new EdgeCount().getAggregatePropertyKey()).getLong());
  }
}
