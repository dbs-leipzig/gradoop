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
package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.runtime.client.JobExecutionException;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasEdgeLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasVertexLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for aggregation on single graphs.
 */
public class AggregationTest extends GradoopFlinkTestBase {
  /**
   * Property key used in aggregations.
   */
  static final String PROPERTY = "p";
  /**
   * Aggregate property key for edge aggregations.
   */
  static final String EDGE_AGGREGATE_PROPERTY = "aggE_p";
  /**
   * Aggregate property key for vertex aggregations.
   */
  static final String VERTEX_AGGREGATE_PROPERTY = "aggV_p";
  /**
   * Aggregate property key for element aggregations.
   */
  static final String ELEMENT_AGGREGATE_PROPERTY = "agg_p";

  /**
   * Test MinProperty, VertexMinProperty and EdgeMinProperty with a single graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphMin() throws Exception {
    LogicalGraph graph = getLoaderFromString("org:Ga[" +
        "(:Va{p : 0.5f})-[:ea{p : 2}]->(:Vb{p : 3.1f})" +
        "(:Vc{p : 0.33f})-[:eb]->(:Vd{p : 0.0f})" +
        "]"
      )
      .getLogicalGraphByVariable("org");

    MinVertexProperty minVertexProperty =
      new MinVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    MinEdgeProperty minEdgeProperty = new MinEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    MinProperty minProperty = new MinProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    graph = graph
      .aggregate(minVertexProperty)
      .aggregate(minEdgeProperty)
      .aggregate(minProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge minimum not set",
      graphHead.hasProperty(minEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex minimum not set",
      graphHead.hasProperty(minVertexProperty.getAggregatePropertyKey()));
    assertTrue("element minimum not set",
      graphHead.hasProperty(minProperty.getAggregatePropertyKey()));
    assertEquals(
      2,
      graphHead.getPropertyValue(
        minEdgeProperty.getAggregatePropertyKey()).getInt());
    assertEquals(
      0.0f,
      graphHead.getPropertyValue(
        minVertexProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
    assertEquals(
      0.0f,
      graphHead.getPropertyValue(
        minProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
  }

  /**
   * Test MaxProperty, VertexMaxProperty and EdgeMaxProperty with a single graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphMax() throws Exception {
    LogicalGraph graph = getLoaderFromString("org:Ga[" +
        "(:Va{p : 0.5f})-[:ea{p : 2}]->(:Vb{p : 3.1f})" +
        "(:Vc{p : 0.33f})-[:eb]->(:Vd{p : 0.0f})" +
        "]"
      )
      .getLogicalGraphByVariable("org");

    MaxVertexProperty maxVertexProperty =
      new MaxVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    MaxProperty maxProperty = new MaxProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    graph = graph
      .aggregate(maxVertexProperty)
      .aggregate(maxEdgeProperty)
      .aggregate(maxProperty);

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
    assertTrue("element maximum not set",
      graphHead.hasProperty(maxProperty.getAggregatePropertyKey()));
    assertEquals(
      3.1f,
      graphHead.getPropertyValue(
        maxProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
  }

  /**
   * Test SumProperty, VertexSumProperty and EdgeSumProperty with a single graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphSum() throws Exception {
    LogicalGraph graph = getLoaderFromString("org:Ga[" +
        "(:Va{p : 0.5f})-[:ea{p : 2}]->(:Vb{p : 3.1f})" +
        "(:Vc{p : 0.33f})-[:eb]->(:Vd{p : 0.0f})" +
          "]"
      ).getLogicalGraphByVariable("org");


    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    SumProperty sumProperty = new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    graph = graph
      .aggregate(sumVertexProperty)
      .aggregate(sumEdgeProperty)
      .aggregate(sumProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set",
      graphHead.hasProperty(sumEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex sum not set",
      graphHead.hasProperty(sumVertexProperty.getAggregatePropertyKey()));
    assertTrue("element sum not set",
      graphHead.hasProperty(sumProperty.getAggregatePropertyKey()));
    assertEquals(
      2,
      graphHead.getPropertyValue(
        sumEdgeProperty.getAggregatePropertyKey()).getInt());
    assertEquals(
      3.93f,
      graphHead.getPropertyValue(
        sumVertexProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
    assertEquals(
      5.93f,
      graphHead.getPropertyValue(
        sumProperty.getAggregatePropertyKey()).getFloat(), 0.00001f);
  }

  /**
   * Test SumProperty, VertexSumProperty and EdgeSumProperty with unsupported property value types.
   */
  @Test
  public void testGraphUnsupportedPropertyValueType() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g[({p : 0})-[{p : 0.0}]->({p : true})-[{p : \"\"}]->({})]");

    LogicalGraph graph = loader.getLogicalGraphByVariable("g");

    try {
      graph
        .aggregate(new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY))
        .aggregate(new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY))
        .aggregate(new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY))
        .getGraphHead().print();
    } catch (Exception e) {
      assertTrue(
        e instanceof JobExecutionException &&
          e.getCause() instanceof UnsupportedTypeException);
    }
  }

  /**
   * Test SumProperty, VertexSumProperty and EdgeSumProperty with a single graph
   * and empty properties.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSumWithEmptyProperties() throws Exception {
    LogicalGraph graph = getLoaderFromString(
        "org:Ga[(:Va)-[:ea]->(:Vb)]"
      )
      .getLogicalGraphByVariable("org");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    SumProperty sumProperty = new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    graph = graph
      .aggregate(sumVertexProperty)
      .aggregate(sumEdgeProperty)
      .aggregate(sumProperty);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set", graphHead.hasProperty(
      sumEdgeProperty.getAggregatePropertyKey()));
    assertTrue("vertex sum not set", graphHead.hasProperty(
      sumVertexProperty.getAggregatePropertyKey()));
    assertTrue("element sum not set", graphHead.hasProperty(
      sumProperty.getAggregatePropertyKey()));

    assertEquals(PropertyValue.NULL_VALUE, graphHead.getPropertyValue(
      sumEdgeProperty.getAggregatePropertyKey()));
    assertEquals(PropertyValue.NULL_VALUE, graphHead.getPropertyValue(
      sumVertexProperty.getAggregatePropertyKey()));
    assertEquals(PropertyValue.NULL_VALUE, graphHead.getPropertyValue(
      sumProperty.getAggregatePropertyKey()));
  }

  /**
   * Test Count, VertexCount and EdgeCount with a single graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphCount() throws Exception {
    LogicalGraph graph = getLoaderFromString("[()-->()<--()]").getLogicalGraph();

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();
    Count count = new Count();

    graph = graph
      .aggregate(vertexCount)
      .aggregate(edgeCount)
      .aggregate(count);

    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("vertex count not set",
      graphHead.hasProperty(vertexCount.getAggregatePropertyKey()));
    assertTrue("edge count not set",
      graphHead.hasProperty(edgeCount.getAggregatePropertyKey()));
    assertTrue("element count not set",
      graphHead.hasProperty(count.getAggregatePropertyKey()));

    assertCounts(graphHead, 3L, 2L);
  }

  /**
   * Helper function to test count aggregators.
   * Asserts the count for vertices, edges and all elements.
   *
   * @param graphHead graph head containing the aggregate properties
   * @param expectedVertexCount expected vertex count
   * @param expectedEdgeCount expected edge count
   */
  void assertCounts(EPGMGraphHead graphHead, long expectedVertexCount, long expectedEdgeCount) {
    assertEquals("wrong vertex count", expectedVertexCount,
      graphHead.getPropertyValue(new VertexCount().getAggregatePropertyKey()).getLong());
    assertEquals("wrong edge count", expectedEdgeCount,
      graphHead.getPropertyValue(new EdgeCount().getAggregatePropertyKey()).getLong());
    assertEquals("wrong element count", expectedEdgeCount + expectedVertexCount,
      graphHead.getPropertyValue(new Count().getAggregatePropertyKey()).getLong());
  }

  /**
   * Test HasVertexLabel with a single graph and result false.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasVertexLabelTrue() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasVertexLabel hasPerson = new HasVertexLabel("Person");
    graph = graph.aggregate(hasPerson);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasVertexLabel_Person not set",
      graphHead.hasProperty(hasPerson.getAggregatePropertyKey()));
    assertTrue("Property hasVertexLabel_Person is false, should be true",
      graphHead.getPropertyValue(hasPerson.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasVertexLabel with a single graph and result false.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasVertexLabelFalse() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasVertexLabel hasNotExistent = new HasVertexLabel("LabelDoesNotExist");
    graph = graph.aggregate(hasNotExistent);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasVertexLabel_LabelDoesNotExist not set",
      graphHead.hasProperty(hasNotExistent.getAggregatePropertyKey()));
    assertFalse("Property hasVertexLabel_LabelDoesNotExist is true, should be false",
      graphHead.getPropertyValue(hasNotExistent.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasEdgeLabel with a single graph and result true.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasEdgeLabelTrue() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasEdgeLabel hasKnows = new HasEdgeLabel("knows");
    graph = graph.aggregate(hasKnows);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasEdgeLabel_knows not set",
      graphHead.hasProperty(hasKnows.getAggregatePropertyKey()));
    assertTrue("Property hasEdgeLabel_knows is false, should be true",
      graphHead.getPropertyValue(hasKnows.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasEdgeLabel with a single graph and result false.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasEdgeLabelFalse() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasEdgeLabel hasNotExistent = new HasEdgeLabel("LabelDoesNotExist");
    graph = graph.aggregate(hasNotExistent);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasEdgeLabel_LabelDoesNotExist not set",
      graphHead.hasProperty(hasNotExistent.getAggregatePropertyKey()));
    assertFalse("Property hasEdgeLabel_LabelDoesNotExist is true, should be false",
      graphHead.getPropertyValue(hasNotExistent.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasEdgeLabel with a single edgeless graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testEdgelessGraphHasEdgeLabel() throws Exception {
    LogicalGraph graph = getLoaderFromString("g0[(v0)(v1)]")
      .getLogicalGraphByVariable("g0");
    HasEdgeLabel hasLabel = new HasEdgeLabel("anyLabel");
    graph = graph.aggregate(hasLabel);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasEdgeLabel_anyLabel not set",
      graphHead.hasProperty(hasLabel.getAggregatePropertyKey()));
    assertNull("Property hasEdgeLabel_anyLabel is not NULL",
      graphHead.getPropertyValue(hasLabel.getAggregatePropertyKey()).getObject());
  }

  /**
   * Test HasVertexLabel with a single graph and empty label string.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphHasVertexLabelEmptyString() throws Exception {
    LogicalGraph graph = getLoaderFromString("g0[(v0)-[e0]->(v1)]")
      .getLogicalGraphByVariable("g0");
    HasVertexLabel hasLabel = new HasVertexLabel("");
    graph = graph.aggregate(hasLabel);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasVertexLabel_ not set",
      graphHead.hasProperty(hasLabel.getAggregatePropertyKey()));
    assertTrue("Property hasVertexLabel_ is false, should be true",
      graphHead.getPropertyValue(hasLabel.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasLabel with a single graph and result true.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasElementLabelTrue() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasLabel hasPerson = new HasLabel("Person");
    graph = graph.aggregate(hasPerson);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasLabel_Person not set",
      graphHead.hasProperty(hasPerson.getAggregatePropertyKey()));
    assertTrue("Property hasLabel_Person is false, should be true",
      graphHead.getPropertyValue(hasPerson.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasLabel with a single graph and result false.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphHasElementLabelFalse() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraphByVariable("g2");
    HasLabel hasNotExistent = new HasLabel("LabelDoesNotExist");
    graph = graph.aggregate(hasNotExistent);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasLabel_LabelDoesNotExist not set",
      graphHead.hasProperty(hasNotExistent.getAggregatePropertyKey()));
    assertFalse("Property hasLabel_LabelDoesNotExist is true, should be false",
      graphHead.getPropertyValue(hasNotExistent.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test HasLabel with a single empty graph.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testEmptyGraphHasElementLabel() throws Exception {
    LogicalGraph graph = getLoaderFromString("g0[]")
      .getLogicalGraphByVariable("g0");
    HasLabel hasLabel = new HasLabel("anyLabel");
    graph = graph.aggregate(hasLabel);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasLabel_anyLabel not set",
      graphHead.hasProperty(hasLabel.getAggregatePropertyKey()));
    assertNull("Property hasLabel_anyLabel is not NULL",
      graphHead.getPropertyValue(hasLabel.getAggregatePropertyKey()).getObject());
  }

  /**
   * Test HasLabel with a single graph and empty label string.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testSingleGraphHasElementLabelEmptyString() throws Exception {
    LogicalGraph graph = getLoaderFromString("g0[(v0)-[e0]->(v1)]")
      .getLogicalGraphByVariable("g0");
    HasLabel hasLabel = new HasLabel("");
    graph = graph.aggregate(hasLabel);
    EPGMGraphHead graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("Property hasLabel_ not set",
      graphHead.hasProperty(hasLabel.getAggregatePropertyKey()));
    assertTrue("Property hasLabel_ is false, should be true",
      graphHead.getPropertyValue(hasLabel.getAggregatePropertyKey()).getBoolean());
  }

  /**
   * Test using multiple vertex aggregation functions in one call.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphWithMultipleVertexAggregationFunctions() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    SumVertexProperty sumVertexProperty = new SumVertexProperty("age");
    MaxVertexProperty maxVertexProperty = new MaxVertexProperty("age");

    LogicalGraph expected = graph.aggregate(sumVertexProperty).aggregate(maxVertexProperty);
    LogicalGraph output = graph.aggregate(sumVertexProperty, maxVertexProperty);

    collectAndAssertTrue(expected.equalsByData(output));
  }

  /**
   * Test using multiple edge aggregation functions in one call.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphWithMultipleEdgeAggregationFunctions() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty("since");
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty("since");

    LogicalGraph expected = graph.aggregate(sumEdgeProperty).aggregate(maxEdgeProperty);
    LogicalGraph output = graph.aggregate(sumEdgeProperty, maxEdgeProperty);

    collectAndAssertTrue(expected.equalsByData(output));
  }

  /**
   * Test using multiple element aggregation functions in one call.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphWithMultipleElementAggregationFunctions() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    SumProperty sumProperty = new SumProperty("since");
    MaxProperty maxProperty = new MaxProperty("since");

    LogicalGraph expected = graph.aggregate(sumProperty).aggregate(maxProperty);
    LogicalGraph output = graph.aggregate(sumProperty, maxProperty);

    collectAndAssertTrue(expected.equalsByData(output));
  }

  /**
   * Test using multiple vertex and edge aggregation functions in one call.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testSingleGraphWithMultipleDifferentAggregationFunctions() throws Exception {
    LogicalGraph graph = getSocialNetworkLoader().getLogicalGraph();

    MinVertexProperty minVertexProperty = new MinVertexProperty("age");
    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty("since");
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty("since");
    MaxProperty maxProperty = new MaxProperty("since", "max2_since");

    LogicalGraph expected = graph.aggregate(minVertexProperty)
      .aggregate(sumEdgeProperty)
      .aggregate(maxEdgeProperty)
      .aggregate(maxProperty);
    LogicalGraph output = graph
      .aggregate(minVertexProperty, sumEdgeProperty, maxEdgeProperty, maxProperty);

    collectAndAssertTrue(expected.equalsByData(output));
  }

  /**
   * Test using multiple aggregation functions in one call on an empty graph.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testEmptySingleGraphWithMultipleAggregationFunctions() throws Exception {
    LogicalGraph graph = getLoaderFromString("g[]").getLogicalGraphByVariable("g");

    VertexCount vertexCount = new VertexCount();
    Count count = new Count();
    MinEdgeProperty minEdgeProperty = new MinEdgeProperty(PROPERTY);

    LogicalGraph expected = graph.aggregate(vertexCount)
      .aggregate(minEdgeProperty)
      .aggregate(count);
    LogicalGraph output = graph.aggregate(vertexCount, minEdgeProperty, count);

    collectAndAssertTrue(expected.equalsByData(output));
  }
}
