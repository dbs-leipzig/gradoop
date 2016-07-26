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

package org.gradoop.model.impl.operators.aggregation;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.model.impl.operators.aggregation.functions.max.MaxEdgeProperty;
import org.gradoop.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.model.impl.operators.aggregation.functions.min.MinEdgeProperty;
import org.gradoop.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends GradoopFlinkTestBase {

  public static final String EDGE_SUM = "edge sum";
  public static final String VERTEX_SUM = "vertex sum";
  public static final String EDGE_PROPERTY = "ep";
  public static final String VERTEX_PROPERTY = "vp";

  public static final String EDGE_MIN = "edge min";
  public static final String VERTEX_MIN = "vertex min";

  public static final String EDGE_MAX = "edge min";
  public static final String VERTEX_MAX = "vertex min";

  public static final String EDGE_COUNT = "edgeCount";
  public static final String VERTEX_COUNT = "vertexCount";

  @Test
  public void testSingleGraphVertexAndEdgeMin() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("" +
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
          "]"
      ).getDatabase().getDatabaseGraph();
    graph = graph
      .aggregate(VERTEX_MIN,
        new MinVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          VERTEX_PROPERTY,
          Float.MAX_VALUE
        ));
    graph = graph
      .aggregate(EDGE_MIN,
        new MinEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          EDGE_PROPERTY,
          Integer.MAX_VALUE
        ));

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge minimum not set", graphHead.hasProperty(EDGE_MIN));
    assertTrue("vertex minimum not set", graphHead.hasProperty(VERTEX_MIN));
    assertEquals(
      2,
      graphHead.getPropertyValue(EDGE_MIN).getInt());
    assertEquals(
      0.0f,
      graphHead.getPropertyValue(VERTEX_MIN).getFloat(), 0.00001f);
  }

  @Test
  public void testCollectionVertexAndEdgeMin() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection
        .apply(new ApplyAggregation<>(VERTEX_MIN,
          new MinVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            VERTEX_PROPERTY,
            Float.MAX_VALUE)))
        .apply(new ApplyAggregation<>(EDGE_MIN,
          new MinEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            EDGE_PROPERTY,
            Long.MAX_VALUE)));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge minimum not set", graphHead.hasProperty(EDGE_MIN));
      assertTrue("vertex minimum not set", graphHead.hasProperty(VERTEX_MIN));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_MIN).getLong());
        assertEquals(
          0.1f,
          graphHead.getPropertyValue(VERTEX_MIN).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_MIN).getLong());
        assertEquals(
          0.3f,
          graphHead.getPropertyValue(VERTEX_MIN).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          Long.MAX_VALUE,
          graphHead.getPropertyValue(EDGE_MIN).getLong());
        assertEquals(
          Float.MAX_VALUE,
          graphHead.getPropertyValue(VERTEX_MIN).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testSingleGraphVertexAndEdgeMax() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("" +
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
          "]"
      ).getDatabase().getDatabaseGraph();
    graph = graph
      .aggregate(VERTEX_MAX,
        new MaxVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          VERTEX_PROPERTY,
          Float.MIN_VALUE
        ));
    graph = graph
      .aggregate(EDGE_MAX,
        new MaxEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          EDGE_PROPERTY,
          Integer.MIN_VALUE
        ));

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge maximum not set", graphHead.hasProperty(EDGE_MAX));
    assertTrue("vertex maximum not set", graphHead.hasProperty(VERTEX_MAX));
    assertEquals(
      2,
      graphHead.getPropertyValue(EDGE_MAX).getInt());
    assertEquals(
      3.1f,
      graphHead.getPropertyValue(VERTEX_MAX).getFloat(), 0.00001f);
  }

  @Test
  public void testCollectionVertexAndEdgeMax() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection
        .apply(new ApplyAggregation<>(VERTEX_MAX,
          new MaxVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            VERTEX_PROPERTY,
            Float.MIN_VALUE)))
        .apply(new ApplyAggregation<>(EDGE_MAX,
          new MaxEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            EDGE_PROPERTY,
            Long.MIN_VALUE)));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge maximum not set", graphHead.hasProperty(EDGE_MAX));
      assertTrue("vertex maximum not set", graphHead.hasProperty(VERTEX_MAX));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2L,
          graphHead.getPropertyValue(EDGE_MAX).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(VERTEX_MAX).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2L,
          graphHead.getPropertyValue(EDGE_MAX).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(VERTEX_MAX).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          Long.MIN_VALUE,
          graphHead.getPropertyValue(EDGE_MAX).getLong());
        assertEquals(
          Float.MIN_VALUE,
          graphHead.getPropertyValue(VERTEX_MAX).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testSingleGraphVertexAndEdgeSum() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("" +
          "org:Ga[" +
          "(:Va{vp=0.5f})-[:ea{ep=2}]->(:Vb{vp=3.1f});" +
          "(:Vc{vp=0.33f})-[:eb]->(:Vd{vp=0.0f})" +
          "]"
      ).getDatabase().getDatabaseGraph();
    graph = graph
      .aggregate(VERTEX_SUM,
        new SumVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          VERTEX_PROPERTY,
          0.0f
        ));
    graph = graph
      .aggregate(EDGE_SUM,
        new SumEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          EDGE_PROPERTY,
          0
        ));

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set", graphHead.hasProperty(EDGE_SUM));
    assertTrue("vertex sum not set", graphHead.hasProperty(VERTEX_SUM));
    assertEquals(
      2,
      graphHead.getPropertyValue(EDGE_SUM).getInt());
    assertEquals(
      3.93f,
      graphHead.getPropertyValue(VERTEX_SUM).getFloat(), 0.00001f);
  }

  @Test
  public void testCollectionVertexAndEdgeSum() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection
        .apply(new ApplyAggregation<>(
          VERTEX_SUM,
          new SumVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            VERTEX_PROPERTY,
            0.0f)))
        .apply(new ApplyAggregation<>(
          EDGE_SUM,
          new SumEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            EDGE_PROPERTY,
            0L)));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge sum not set", graphHead.hasProperty(EDGE_SUM));
      assertTrue("vertex sum not set", graphHead.hasProperty(VERTEX_SUM));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_SUM).getLong());
        assertEquals(
          0.9f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_SUM).getLong());
        assertEquals(
          0.8f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          0,
          graphHead.getPropertyValue(EDGE_SUM).getInt());
        assertEquals(
          0f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testWithMixedTypePropertyValues() throws Exception{
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
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

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection
        .apply(new ApplyAggregation<>(
          VERTEX_SUM,
          new SumVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            VERTEX_PROPERTY,
            0.0f)))
        .apply(new ApplyAggregation<>(
          EDGE_SUM,
          new SumEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
            EDGE_PROPERTY,
            0L)));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      assertTrue("edge sum not set", graphHead.hasProperty(EDGE_SUM));
      assertTrue("vertex sum not set", graphHead.hasProperty(VERTEX_SUM));
      if (graphHead.getId().equals(g0Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_SUM).getLong());
        assertEquals(
          0.6f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g1Id)) {
        assertEquals(
          2,
          graphHead.getPropertyValue(EDGE_SUM).getLong());
        assertEquals(
          0.5f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      } else if (graphHead.getId().equals(g2Id)) {
        assertEquals(
          0,
          graphHead.getPropertyValue(EDGE_SUM).getInt());
        assertEquals(
          0f,
          graphHead.getPropertyValue(VERTEX_SUM).getFloat(),
          0.00001);
      }  else {
        Assert.fail("unexpected graph head: " + graphHead);
      }
    }
  }

  @Test
  public void testSumWithEmptyProperties() throws Exception {
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("" +
        "org:Ga[(:Va)-[:ea]->(:Vb)]"
      ).getDatabase().getDatabaseGraph();
    graph = graph
      .aggregate(VERTEX_SUM,
        new SumVertexProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          VERTEX_PROPERTY,
          0
        ));
    graph = graph
      .aggregate(EDGE_SUM,
        new SumEdgeProperty<GraphHeadPojo, VertexPojo, EdgePojo>(
          EDGE_PROPERTY,
          0
        ));

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("edge sum not set", graphHead.hasProperty(EDGE_SUM));
    assertTrue("vertex sum not set", graphHead.hasProperty(VERTEX_SUM));
    assertEquals(
      0,
      graphHead.getPropertyValue(EDGE_SUM).getInt());
    assertEquals(
      0,
      graphHead.getPropertyValue(VERTEX_SUM).getInt());
  }


  @Test
  public void testSingleGraphVertexAndEdgeCount() throws Exception {

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      getLoaderFromString("[()-->()<--()]").getDatabase().getDatabaseGraph();

    graph = graph
      .aggregate(VERTEX_COUNT,
        new VertexCount<GraphHeadPojo, VertexPojo, EdgePojo>())
      .aggregate(EDGE_COUNT,
        new EdgeCount<GraphHeadPojo, VertexPojo, EdgePojo>()
      );

    GraphHeadPojo graphHead = graph.getGraphHead().collect().get(0);

    assertTrue("vertex count not set", graphHead.hasProperty(VERTEX_COUNT));
    assertTrue("edge count not set", graphHead.hasProperty(EDGE_COUNT));
    assertCounts(graphHead, 3L, 2L);
  }

  @Test
  public void testCollectionVertexAndEdgeCount() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
      "g0[()-->()<--()]" +
      "g1[()-->()-->()-->()]" +
      "g2[()-->()]" +
      "g3[]");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection
        .apply(new ApplyAggregation<>(VERTEX_COUNT,
          new VertexCount<GraphHeadPojo, VertexPojo, EdgePojo>()))
        .apply(new ApplyAggregation<>(EDGE_COUNT,
          new EdgeCount<GraphHeadPojo, VertexPojo, EdgePojo>()));

    GradoopId g0Id = loader.getGraphHeadByVariable("g0").getId();
    GradoopId g1Id = loader.getGraphHeadByVariable("g1").getId();
    GradoopId g2Id = loader.getGraphHeadByVariable("g2").getId();
    GradoopId g3Id = loader.getGraphHeadByVariable("g3").getId();

    int graphHeadCount = 0;

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
      graphHeadCount++;
      assertTrue("vertex count not set", graphHead.hasProperty(VERTEX_COUNT));
      assertTrue("edge count not set", graphHead.hasProperty(EDGE_COUNT));
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
      graphHead.getPropertyValue(VERTEX_COUNT).getLong());
    assertEquals("wrong edge count", expectedEdgeCount,
      graphHead.getPropertyValue(EDGE_COUNT).getLong());
  }
}
