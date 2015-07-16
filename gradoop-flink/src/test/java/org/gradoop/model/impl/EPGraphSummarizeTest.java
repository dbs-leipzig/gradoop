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

package org.gradoop.model.impl;

import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.impl.operators.Summarization;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.*;

public class EPGraphSummarizeTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphSummarizeTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testSummarizeWithVertexGroupingKeySymmetricGraph() throws
    Exception {

    EPGraph inputGraph = graphStore.getGraph(2L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph = inputGraph.summarize(vertexGroupingKey);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // two summarized vertices:
    // 0 __VERTEX__ {city: "Leipzig", count: 2}
    // 2 __VERTEX__ {city: "Dresden", count: 2}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L;

    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // four summarized edges:
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 1}
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    assertEquals("wrong number of edges", 4L, summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[__EDGE__]-> Leipzig {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeWithVertexGroupingKeyAsymmetricGraph() throws
    Exception {

    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph = inputGraph.summarize(vertexGroupingKey);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // three summarized vertices:
    // 0 __VERTEX__ {city: "Leipzig", count: 2}
    // 2 __VERTEX__ {city: "Dresden", count: 3}
    // 5 __VERTEX__ {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Berlin", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // five summarized edges:
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeWithAbsentVertexGroupingKey() throws Exception {
    EPGraph input = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph = input.summarize(vertexGroupingKey);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // two summarized vertices:
    // 2 __VERTEX__ {city: "Dresden", count: 2}
    // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDDresden = 2L, vertexIDGraphProcessingForum = 10L;
    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDDresden)) {
        // 2 __VERTEX__ {city: "Dresden", count: 2}
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDGraphProcessingForum)) {
        // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          Summarization.NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // two summarized edges:
    // [16] Default -[__EDGE__]-> Dresden {count: 3}
    // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
    assertEquals("wrong number of edges", 2L, summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(16L)) {
        // Default -[__EDGE__]-> Dresden {count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL,
          vertexIDGraphProcessingForum, vertexIDDresden, aggregatePropertyKey,
          3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(4L)) {
        // Dresden -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeWithVertexAndEdgeGroupingKeyAsymmetricGraph() throws
    Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph =
      inputGraph.summarize(vertexGroupingKey, edgeGroupingKey);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // three summarized vertices:
    // 0 __VERTEX__ {city: "Leipzig", count: 2}
    // 2 __VERTEX__ {city: "Dresden", count: 3}
    // 5 __VERTEX__ {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Berlin", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // six summarized edges:
    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
    // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
    // [0] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDDresden, edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeWithAbsentVertexAndEdgeGroupingKey() throws
    Exception {
    EPGraph input = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph =
      input.summarize(vertexGroupingKey, edgeGroupingKey);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // two summarized vertices:
    // 2 __VERTEX__ {city: "Dresden", count: 2}
    // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDDresden = 2L, vertexIDGraphProcessingForum = 10L;
    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDDresden)) {
        // 2 __VERTEX__ {city: "Dresden", count: 2}
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDGraphProcessingForum)) {
        // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
        testVertex(v, FlinkConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          Summarization.NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // three summarized edges:
    // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
    // [19] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
    assertEquals("wrong number of edges", 3L, summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(16L)) {
        // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL,
          vertexIDGraphProcessingForum, vertexIDDresden, edgeGroupingKey,
          "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(19L)) {
        // [19] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL,
          vertexIDGraphProcessingForum, vertexIDDresden, edgeGroupingKey,
          Summarization.NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  private void testVertex(EPVertexData vertex, String expectedVertexLabel,
    String vertexGroupingKey, String expectedVertexGroupingValue,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    assertEquals("wrong vertex label", expectedVertexLabel, vertex.getLabel());
    assertEquals("wrong property value", expectedVertexGroupingValue,
      vertex.getProperty(vertexGroupingKey));
    assertEquals("wrong vertex property", expectedCountValue,
      vertex.getProperty(aggregatePropertyKey));
    assertEquals("wrong number of graphs", expectedGraphCount,
      vertex.getGraphs().size());
    assertTrue("wrong graph id", vertex.getGraphs().contains(expectedGraphID));
  }

  private void testEdge(EPEdgeData edge, String expectedEdgeLabel,
    Long expectedSourceVertex, Long expectedTargetVertex,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    testEdge(edge, expectedEdgeLabel, expectedSourceVertex,
      expectedTargetVertex, null, null, aggregatePropertyKey,
      expectedCountValue, expectedGraphCount, expectedGraphID);
  }

  private void testEdge(EPEdgeData edge, String expectedEdgeLabel,
    Long expectedSourceVertex, Long expectedTargetVertex,
    String edgeGroupingKey, String expectedGroupingValue,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    assertEquals("wrong edge label", expectedEdgeLabel, edge.getLabel());
    assertEquals("wrong source vertex", expectedSourceVertex,
      edge.getSourceVertex());
    assertEquals("wrong target vertex", expectedTargetVertex,
      edge.getTargetVertex());
    assertEquals("wrong edge property", expectedCountValue,
      edge.getProperty(aggregatePropertyKey));
    assertEquals("wrong number of graphs", expectedGraphCount,
      edge.getGraphs().size());
    assertTrue("wrong graph id", edge.getGraphs().contains(expectedGraphID));

    if (edgeGroupingKey != null && expectedGroupingValue != null) {
      assertEquals("wrong group value", expectedGroupingValue,
        eve.getProperty(edgeGroupingKey));
    }
  }
}
