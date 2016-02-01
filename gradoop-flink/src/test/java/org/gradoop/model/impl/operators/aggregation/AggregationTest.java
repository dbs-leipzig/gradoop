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
import org.gradoop.model.impl.operators.aggregation.functions.EdgeCount;
import org.gradoop.model.impl.operators.aggregation.functions.VertexCount;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Assert;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregationTest extends GradoopFlinkTestBase {

  public static final String EDGE_COUNT = "edgeCount";
  public static final String VERTEX_COUNT = "vertexCount";

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
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

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

    for (EPGMGraphHead graphHead : outputCollection.getGraphHeads().collect()) {
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
  }

  private void assertCounts(EPGMGraphHead graphHead,
    long expectedVertexCount, long expectedEdgeCount) {
    assertEquals("wrong vertex count", expectedVertexCount,
      graphHead.getPropertyValue(VERTEX_COUNT).getLong());
    assertEquals("wrong edge count", expectedEdgeCount,
      graphHead.getPropertyValue(EDGE_COUNT).getLong());
  }
}
