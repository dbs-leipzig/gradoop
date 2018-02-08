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
  
  public static final String EDGE_PROPERTY = "ep";
  public static final String VERTEX_PROPERTY = "vp";

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
  public void testCollectionVertexAndEdgeMin() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "g0[" +
        "(va {vp : 0.5})" +
        "(vb {vp : 0.3})" +
        "(vc {vp : 0.1})" +
        "(va)-[ea {ep : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    MinVertexProperty minVertexProperty =
      new MinVertexProperty(VERTEX_PROPERTY);

    MinEdgeProperty minEdgeProperty =
      new MinEdgeProperty(EDGE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(minVertexProperty))
      .apply(new ApplyAggregation(minEdgeProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge minimum not set", graphHead.hasProperty
        (minEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex minimum not set", graphHead.hasProperty
        (minVertexProperty.getAggregatePropertyKey()));

      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(minEdgeProperty.getAggregatePropertyKey());

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(minVertexProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.1f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.3f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
      } else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
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
  public void testCollectionVertexAndEdgeMax() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "g0[" +
        "(va {vp : 0.5f})" +
        "(vb {vp : 0.3f})" +
        "(vc {vp : 0.1f})" +
        "(va)-[ea {ep : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    MaxVertexProperty maxVertexProperty =
      new MaxVertexProperty(VERTEX_PROPERTY);

    MaxEdgeProperty maxEdgeProperty =
      new MaxEdgeProperty(EDGE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(maxVertexProperty))
      .apply(new ApplyAggregation(maxEdgeProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge maximum not set",
        graphHead.hasProperty(maxEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex maximum not set",
        graphHead.hasProperty(maxVertexProperty.getAggregatePropertyKey()));


      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(maxEdgeProperty.getAggregatePropertyKey());


      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(maxVertexProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2L, edgeAggregate.getLong());
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2L, edgeAggregate.getLong());
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
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
  public void testCollectionVertexAndEdgeSum() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "g0[" +
        "(va {vp : 0.5})" +
        "(vb {vp : 0.3})" +
        "(vc {vp : 0.1})" +
        "(va)-[ea {ep : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(VERTEX_PROPERTY);

    SumEdgeProperty sumEdgeProperty =
      new SumEdgeProperty(EDGE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(sumVertexProperty))
      .apply(new ApplyAggregation(sumEdgeProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(sumVertexProperty.getAggregatePropertyKey());
      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(sumEdgeProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.9f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.8f, vertexAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE,vertexAggregate);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testWithMixedTypePropertyValues() throws Exception{
    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "g0[" +
        "(va {vp : 0.5})" +
        "(vc {vp : 1})" +
        "(va)-[ea {ep : 2L}]->(vb)" +
        "(vb)-[eb {ep : 2.0F}]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(VERTEX_PROPERTY);

    SumEdgeProperty sumEdgeProperty =
      new SumEdgeProperty(EDGE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(sumVertexProperty))
      .apply(new ApplyAggregation(sumEdgeProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    List<GraphHead> graphHeads = outputCollection.getGraphHeads().collect();

    for (EPGMGraphHead graphHead : graphHeads) {
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(sumVertexProperty.getAggregatePropertyKey());

      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(sumEdgeProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(1.5d, vertexAggregate.getDouble(), 0.00001);
        assertEquals(
          new BigDecimal("4.0"),
          edgeAggregate.getBigDecimal()
            .round(new MathContext(2, RoundingMode.HALF_UP)));
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2L, edgeAggregate.getLong());
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
      }
    }
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
  public void testCollectionUnsupportedPropertyValueType() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g[({a : 0})-[{b : 0.0}]->({a : true})-[{b : \"\"}]->({})]");

    GraphCollection collection = loader.getGraphCollectionByVariables("g");

    try {
      collection
        .apply(new ApplyAggregation(new SumVertexProperty("a")))
        .apply(new ApplyAggregation(new SumEdgeProperty("b")))
        .getGraphHeads().print();
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

  @Test
  public void testCollectionVertexAndEdgeCount() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[()-->()<--()]" +
      "g1[()-->()-->()-->()]" +
      "g2[()-->()]" +
      "g3[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(vertexCount))
      .apply(new ApplyAggregation(edgeCount));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();
    GradoopId g3Id = loader.getGraphHeadByVariable("g3").getId();

    int graphHeadCount = 0;

    List<GraphHead> graphHeads = outputCollection.getGraphHeads().collect();

    for (EPGMGraphHead graphHead : graphHeads) {
      graphHeadCount++;
      assertTrue("vertex count not set", graphHead.hasProperty(
        vertexCount.getAggregatePropertyKey()));
      assertTrue("edge count not set", graphHead.hasProperty(
        edgeCount.getAggregatePropertyKey()));

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(vertexCount.getAggregatePropertyKey());

      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(edgeCount.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(3, vertexAggregate.getLong());
        assertEquals(2, edgeAggregate.getLong());
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(4, vertexAggregate.getLong());
        assertEquals(3, edgeAggregate.getLong());
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(2, vertexAggregate.getLong());
        assertEquals(1, edgeAggregate.getLong());
      } else if (graphHead
        .getId().equals(g3Id)) {
        assertCounts(graphHead, 0, 0);
      } else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }

    assertTrue("wrong number of output graph heads", graphHeadCount == 4);
  }

  private void assertCounts(EPGMGraphHead graphHead,
    long expectedVertexCount, long expectedEdgeCount) {

    assertEquals("wrong vertex count", expectedVertexCount,
      graphHead.getPropertyValue(
        new VertexCount().getAggregatePropertyKey()).getLong());
    assertEquals("wrong edge count", expectedEdgeCount,
      graphHead.getPropertyValue(
        new EdgeCount().getAggregatePropertyKey()).getLong());
  }
}
