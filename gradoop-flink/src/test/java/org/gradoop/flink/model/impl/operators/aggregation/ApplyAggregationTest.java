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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
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
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for aggregation on graph collections.
 */
public abstract class ApplyAggregationTest extends AggregationTest {

  /**
   * Test MinProperty, MinVertexProperty and MinEdgeProperty with a graph collection.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testCollectionMin() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
        "(va {p : 0.5})" +
        "(vb {p : 0.3})" +
        "(vc {p : 0.1})" +
        "(va)-[ea {p : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    MinVertexProperty minVertexProperty =
      new MinVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    MinEdgeProperty minEdgeProperty = new MinEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    MinProperty minProperty = new MinProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(minVertexProperty))
      .apply(new ApplyAggregation(minEdgeProperty))
      .apply(new ApplyAggregation(minProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge minimum not set", graphHead.hasProperty
        (minEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex minimum not set", graphHead.hasProperty
        (minVertexProperty.getAggregatePropertyKey()));
      assertTrue("element minimum not set", graphHead.hasProperty
        (minProperty.getAggregatePropertyKey()));

      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(minEdgeProperty.getAggregatePropertyKey());
      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(minVertexProperty.getAggregatePropertyKey());
      PropertyValue elementAggregate =
        graphHead.getPropertyValue(minProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.1f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(0.1f, elementAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.3f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(0.3f, elementAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
        assertEquals(PropertyValue.NULL_VALUE, elementAggregate);
      } else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  /**
   * Test MaxProperty, MaxVertexProperty and MaxEdgeProperty with a graph collection.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testCollectionMax() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
        "(va {p : 0.5f})" +
        "(vb {p : 0.3f})" +
        "(vc {p : 0.1f})" +
        "(va)-[ea {p : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    MaxVertexProperty maxVertexProperty =
      new MaxVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    MaxProperty maxProperty = new MaxProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(maxVertexProperty))
      .apply(new ApplyAggregation(maxEdgeProperty))
      .apply(new ApplyAggregation(maxProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge maximum not set",
        graphHead.hasProperty(maxEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex maximum not set",
        graphHead.hasProperty(maxVertexProperty.getAggregatePropertyKey()));
      assertTrue("element maximum not set",
        graphHead.hasProperty(maxVertexProperty.getAggregatePropertyKey()));

      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(maxEdgeProperty.getAggregatePropertyKey());
      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(maxVertexProperty.getAggregatePropertyKey());
      PropertyValue elementAggregate =
        graphHead.getPropertyValue(maxProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2L, edgeAggregate.getLong());
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2L, elementAggregate.getLong());
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2L, edgeAggregate.getLong());
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2L, elementAggregate.getLong());
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
        assertEquals(PropertyValue.NULL_VALUE, elementAggregate);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  /**
   * Test SumProperty, SumVertexProperty and SumEdgeProperty with a graph collection.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testCollectionSum() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
        "(va {p : 0.5})" +
        "(vb {p : 0.3})" +
        "(vc {p : 0.1})" +
        "(va)-[ea {p : 2L}]->(vb)" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    SumProperty sumProperty = new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(sumVertexProperty))
      .apply(new ApplyAggregation(sumEdgeProperty))
      .apply(new ApplyAggregation(sumProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("element sum not set", graphHead.hasProperty(
        sumProperty.getAggregatePropertyKey()));

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(sumVertexProperty.getAggregatePropertyKey());
      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(sumEdgeProperty.getAggregatePropertyKey());
      PropertyValue elementAggregate =
        graphHead.getPropertyValue(sumProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.9f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2.9f, elementAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(2, edgeAggregate.getLong());
        assertEquals(0.8f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2.8f, elementAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
        assertEquals(PropertyValue.NULL_VALUE, elementAggregate);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  /**
   * Test SumProperty, SumVertexProperty and SumEdgeProperty with a graph collection
   * and mixed type property values.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testWithMixedTypePropertyValues() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
        "(va {p : 0.5})" +
        "(vc {p : 1})" +
        "(va)-[ea {p : 2L}]->(vb)" +
        "(vb)-[eb {p : 2.0F}]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb)" +
        "]" +
        "g2[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    SumVertexProperty sumVertexProperty =
      new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY);
    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY);
    SumProperty sumProperty = new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY);

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(sumVertexProperty))
      .apply(new ApplyAggregation(sumEdgeProperty))
      .apply(new ApplyAggregation(sumProperty));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    List<GraphHead> graphHeads = outputCollection.getGraphHeads().collect();

    for (EPGMGraphHead graphHead : graphHeads) {
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      assertTrue("element sum not set", graphHead.hasProperty(
        sumProperty.getAggregatePropertyKey()));

      PropertyValue vertexAggregate =
        graphHead.getPropertyValue(sumVertexProperty.getAggregatePropertyKey());
      PropertyValue edgeAggregate =
        graphHead.getPropertyValue(sumEdgeProperty.getAggregatePropertyKey());
      PropertyValue elementAggregate =
        graphHead.getPropertyValue(sumProperty.getAggregatePropertyKey());

      if (graphHead.getId().equals(g0Id)) {
        assertEquals(1.5f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(
          new BigDecimal("4.0"),
          edgeAggregate.getBigDecimal()
            .round(new MathContext(2, RoundingMode.HALF_UP)));
        assertEquals(
          new BigDecimal("5.5"),
          elementAggregate.getBigDecimal()
            .round(new MathContext(2, RoundingMode.HALF_UP)));
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(0.5f, vertexAggregate.getFloat(), 0.00001);
        assertEquals(2L, edgeAggregate.getLong());
        assertEquals(2.5f, elementAggregate.getFloat(), 0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE, vertexAggregate);
        assertEquals(PropertyValue.NULL_VALUE, edgeAggregate);
        assertEquals(PropertyValue.NULL_VALUE, elementAggregate);
      }
    }
  }

  /**
   * Test SumProperty, SumVertexProperty and SumEdgeProperty with a graph collection
   * and unsupported property types.
   */
  @Test
  public void testCollectionUnsupportedPropertyValueType() {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g[({p : 0})-[{p : 0.0}]->({p : true})-[{p : \"\"}]->({})]");

    GraphCollection collection = loader.getGraphCollectionByVariables("g");

    try {
      collection
        .apply(new ApplyAggregation(new SumVertexProperty(PROPERTY, VERTEX_AGGREGATE_PROPERTY)))
        .apply(new ApplyAggregation(new SumEdgeProperty(PROPERTY, EDGE_AGGREGATE_PROPERTY)))
        .apply(new ApplyAggregation(new SumProperty(PROPERTY, ELEMENT_AGGREGATE_PROPERTY)))
        .getGraphHeads().print();
    } catch (Exception e) {
      assertTrue(
        e instanceof JobExecutionException &&
          e.getCause() instanceof UnsupportedTypeException);
    }
  }

  /**
   * Test Count, VertexCount and EdgeCount with a graph collection.
   *
   * @throws Exception if the execution fails.
   */
  @Test
  public void testCollectionCount() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[()-->()<--()]" +
        "g1[()-->()-->()-->()]" +
        "g2[()-->()]" +
        "g3[]");

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();
    Count count = new Count();

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyAggregation(vertexCount))
      .apply(new ApplyAggregation(edgeCount))
      .apply(new ApplyAggregation(count));

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
      assertTrue("element count not set", graphHead.hasProperty(
        count.getAggregatePropertyKey()));

      if (graphHead.getId().equals(g0Id)) {
        assertCounts(graphHead, 3, 2);
      } else if (graphHead.getId().equals(g1Id)) {
        assertCounts(graphHead, 4, 3);
      } else if (graphHead.getId().equals(g2Id)) {
        assertCounts(graphHead, 2, 1);
      } else if (graphHead
        .getId().equals(g3Id)) {
        assertCounts(graphHead, 0, 0);
      } else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }

    assertTrue("wrong number of output graph heads", graphHeadCount == 4);
  }

  /**
   * Test HasLabel, HasVertexLabel and HasEdgeLabel with a graph collection and result true.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionHasLabelTrue() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    HasVertexLabel hasPerson = new HasVertexLabel("Person");
    HasEdgeLabel hasKnows = new HasEdgeLabel("knows");
    HasLabel hasKnowsElement = new HasLabel("knows");

    collection = collection
      .apply(new ApplyAggregation(hasKnows))
      .apply(new ApplyAggregation(hasKnowsElement))
      .apply(new ApplyAggregation(hasPerson));

    List<GraphHead> graphHeads = collection.getGraphHeads().collect();

    for (EPGMGraphHead graphHead : graphHeads) {
      // check vertex label
      assertTrue("Property hasVertexLabel_Person not set",
        graphHead.hasProperty(hasPerson.getAggregatePropertyKey()));
      assertTrue("Property hasVertexLabel_Person is false, should be true",
        graphHead.getPropertyValue(hasPerson.getAggregatePropertyKey()).getBoolean());
      // check edge label
      assertTrue("Property hasEdgeLabel_knows not set",
        graphHead.hasProperty(hasKnows.getAggregatePropertyKey()));
      assertTrue("Property hasEdgeLabel_knows is false, should be true",
        graphHead.getPropertyValue(hasKnows.getAggregatePropertyKey()).getBoolean());
      // check element label
      assertTrue("Property hasLabel_knows not set",
        graphHead.hasProperty(hasKnowsElement.getAggregatePropertyKey()));
      assertTrue("Property hasLabel_knows is false, should be true",
        graphHead.getPropertyValue(hasKnowsElement.getAggregatePropertyKey()).getBoolean());
    }
  }

  /**
   * Test HasLabel, HasVertexLabel and HasEdgeLabel with a graph collection and result false.
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionHasLabelFalse() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    HasVertexLabel hasSomeLabel = new HasVertexLabel("someLabel");
    HasEdgeLabel hasOtherLabel = new HasEdgeLabel("otherLabel");
    HasLabel hasThirdLabel = new HasLabel("thirdLabel");

    collection = collection
      .apply(new ApplyAggregation(hasSomeLabel))
      .apply(new ApplyAggregation(hasOtherLabel))
      .apply(new ApplyAggregation(hasThirdLabel));

    List<GraphHead> graphHeads = collection.getGraphHeads().collect();

    for (EPGMGraphHead graphHead : graphHeads) {
      // check edge label
      assertTrue("Property hasEdgeLabel_otherLabel not set",
        graphHead.hasProperty(hasOtherLabel.getAggregatePropertyKey()));
      assertFalse("Property hasEdgeLabel_otherLabel is true, should be false",
        graphHead.getPropertyValue(hasOtherLabel.getAggregatePropertyKey()).getBoolean());
      // check vertex label
      assertTrue("Property hasVertexLabel_someLabel not set",
        graphHead.hasProperty(hasSomeLabel.getAggregatePropertyKey()));
      assertFalse("Property hasVertexLabel_someLabel is true, should be false",
        graphHead.getPropertyValue(hasSomeLabel.getAggregatePropertyKey()).getBoolean());
      // check element label
      assertTrue("Property hasLabel_thirdLabel not set",
        graphHead.hasProperty(hasThirdLabel.getAggregatePropertyKey()));
      assertFalse("Property hasLabel_thirdLabel is true, should be false",
        graphHead.getPropertyValue(hasThirdLabel.getAggregatePropertyKey()).getBoolean());
    }
  }

  /**
   * Test using multiple vertex aggregation functions on a graph collection
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionWithMultipleVertexAggregationFunctions() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    SumVertexProperty sumVertexProperty = new SumVertexProperty("age");
    MaxVertexProperty maxVertexProperty = new MaxVertexProperty("age");

    GraphCollection expected = collection.apply(new ApplyAggregation(sumVertexProperty))
      .apply(new ApplyAggregation(maxVertexProperty));
    GraphCollection output = collection.apply(new ApplyAggregation(sumVertexProperty,
      maxVertexProperty));

    collectAndAssertTrue(expected.equalsByGraphData(output));
  }

  /**
   * Test using multiple edge aggregation functions on a graph collection
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionWithMultipleEdgeAggregationFunctions() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    SumEdgeProperty sumEdgeProperty = new SumEdgeProperty("since");
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty("since");

    GraphCollection expected = collection.apply(new ApplyAggregation(sumEdgeProperty))
      .apply(new ApplyAggregation(maxEdgeProperty));
    GraphCollection output = collection.apply(new ApplyAggregation(sumEdgeProperty,
      maxEdgeProperty));

    collectAndAssertTrue(expected.equalsByGraphData(output));
  }

  /**
   * Test using multiple element aggregation functions on a graph collection
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionWithMultipleElementAggregationFunctions() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    SumProperty sumProperty = new SumProperty("since");
    MaxProperty maxProperty = new MaxProperty("since");

    GraphCollection expected = collection.apply(new ApplyAggregation(sumProperty))
      .apply(new ApplyAggregation(maxProperty));
    GraphCollection output = collection.apply(new ApplyAggregation(sumProperty, maxProperty));

    collectAndAssertTrue(expected.equalsByGraphData(output));
  }

  /**
   * Test using multiple vertex and edge aggregation functions on a graph collection
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testCollectionWithMultipleDifferentAggregationFunctions() throws Exception {
    GraphCollection collection = getSocialNetworkLoader()
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();
    Count count = new Count();
    MaxEdgeProperty maxEdgeProperty = new MaxEdgeProperty("since");

    GraphCollection expected = collection
      .apply(new ApplyAggregation(vertexCount))
      .apply(new ApplyAggregation(edgeCount))
      .apply(new ApplyAggregation(count))
      .apply(new ApplyAggregation(maxEdgeProperty));
    GraphCollection output = collection.apply(new ApplyAggregation(vertexCount, edgeCount, count,
      maxEdgeProperty));

    collectAndAssertTrue(expected.equalsByGraphData(output));
  }

  /**
   * Test using multiple aggregation functions on an empty graph collection
   *
   * @throws Exception if the execution or IO fails.
   */
  @Test
  public void testEmptyCollectionWithMultipleAggregationFunctions() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    GraphCollection collection = loader.getGraphCollectionByVariables("g0")
      .difference(loader.getGraphCollectionByVariables("g1"));

    VertexCount vertexCount = new VertexCount();
    EdgeCount edgeCount = new EdgeCount();
    Count count = new Count();

    GraphCollection expected = collection
      .apply(new ApplyAggregation(vertexCount))
      .apply(new ApplyAggregation(edgeCount))
      .apply(new ApplyAggregation(count));
    GraphCollection output = collection.apply(new ApplyAggregation(vertexCount, edgeCount, count));

    collectAndAssertTrue(expected.equalsByGraphData(output));
  }
}
