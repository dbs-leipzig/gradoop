/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.aggregation;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends GradoopFlinkTestBase {
  
  public static final String EDGE_PROPERTY = "ep";
  public static final String VERTEX_PROPERTY = "vp";

  @Test
  public void testSingleGraphVertexAndEdgeMin() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
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
        "(va {vp=0.5});" +
        "(vb {vp=0.3});" +
        "(vc {vp=0.1});" +
        "(va)-[ea {ep=2L}]->(vb);" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb);" +
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
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            minEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.1f,
          graphHead.getPropertyValue(
            minVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            minEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.3f,
          graphHead.getPropertyValue(
            minVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          Long.MAX_VALUE,
          graphHead.getPropertyValue(
            minEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          Float.MAX_VALUE,
          graphHead.getPropertyValue(
            minVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testSingleGraphVertexAndEdgeMax() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
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
        "(va {vp=0.5f});" +
        "(vb {vp=0.3f});" +
        "(vc {vp=0.1f});" +
        "(va)-[ea {ep=2L}]->(vb);" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb);" +
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
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2L,
          graphHead.getPropertyValue(
            maxEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(
            maxVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2L,
          graphHead.getPropertyValue(
            maxEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(
            maxVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          Long.MIN_VALUE,
          graphHead.getPropertyValue(
            maxEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          Float.MIN_VALUE,
          graphHead.getPropertyValue(
            maxVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testSingleGraphVertexAndEdgeSum() throws Exception {
    LogicalGraph graph = getLoaderFromString(
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
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
        "(va {vp=0.5});" +
        "(vb {vp=0.3});" +
        "(vc {vp=0.1});" +
        "(va)-[ea {ep=2L}]->(vb);" +
        "(vb)-[eb]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb);" +
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
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.9f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.8f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          0,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getInt());
        assertEquals(
          0f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testWithMixedTypePropertyValues() throws Exception{
    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "g0[" +
        "(va {vp=0.5});" +
        "(vc {vp=0.1});" +
        "(va)-[ea {ep=2L}]->(vb);" +
        "(vb)-[eb {ep=2.0F}]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb);" +
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
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      if (graphHead.getId().equals(g0Id)) {

        System.out.println(graphHead.getPropertyValue(
          sumEdgeProperty.getAggregatePropertyKey()));

        assertEquals(
          4.0f,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
        assertEquals(
          0.6f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          PropertyValue.NULL_VALUE,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()));
        assertEquals(
          PropertyValue.NULL_VALUE,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()));
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(PropertyValue.NULL_VALUE,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()));
        assertEquals(
          PropertyValue.NULL_VALUE,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()));
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testUnsupportedPropertyValueType() throws Exception{
    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g0[" +
        "(va {vp=0.5});" +
        "(vb {vp=\"test\"});" +
        "(vc {vp=0.1});" +
        "(va)-[ea {ep=2L}]->(vb);" +
        "(vb)-[eb {ep=2.0F}]->(vc)" +
        "]" +
        "g1[" +
        "(va)-[ea]->(vb);" +
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
      assertTrue("edge sum not set", graphHead.hasProperty(
        sumEdgeProperty.getAggregatePropertyKey()));
      assertTrue("vertex sum not set", graphHead.hasProperty(
        sumVertexProperty.getAggregatePropertyKey()));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.6f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          0,
          graphHead.getPropertyValue(
            sumEdgeProperty.getAggregatePropertyKey()).getInt());
        assertEquals(
          0f,
          graphHead.getPropertyValue(
            sumVertexProperty.getAggregatePropertyKey()).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
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
    assertEquals(
      0,
      graphHead.getPropertyValue(
        sumEdgeProperty.getAggregatePropertyKey()).getInt());
    assertEquals(
      0,
      graphHead.getPropertyValue(
        sumVertexProperty.getAggregatePropertyKey()).getInt());
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

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      graphHeadCount++;
      assertTrue("vertex count not set", graphHead.hasProperty(
        vertexCount.getAggregatePropertyKey()));
      assertTrue("edge count not set", graphHead.hasProperty(
        edgeCount.getAggregatePropertyKey()));
      if (graphHead.getId().equals(g0Id)) {
        assertCounts(graphHead, 3, 2);
      } else if (graphHead.getId().equals(g1Id)) {
        assertCounts(graphHead, 4, 3);
      } else if (graphHead.getId().equals(g2Id)) {
        assertCounts(graphHead, 2, 1);
      } else if (graphHead.getId().equals(g3Id)) {
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
