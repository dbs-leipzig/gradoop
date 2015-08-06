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

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.impl.operators.Summarization;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.gradoop.model.impl.operators.Summarization.NULL_VALUE;
import static org.junit.Assert.*;

public abstract class EPGraphSummarizeTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphSummarizeTest() {
    this.graphStore = createSocialGraph();
  }

  public abstract Summarization getSummarizationImpl(String vertexGroupingKey,
    boolean useVertexLabel, String edgeGroupingKey, boolean useEdgeLabel);

  @Test
  public void testSummarizeOnVertexPropertySymmetricGraph() throws Exception {

    EPGraph inputGraph = graphStore.getGraph(2L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);

    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 2 summarized vertices:
    // [0] __VERTEX__ {city: "Leipzig", count: 2}
    // [2] __VERTEX__ {city: "Dresden", count: 2}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L;

    for (VertexData v : summarizedGraph.getVertices().collect()) {
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

    // 4 summarized sna_edges:
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 1}
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    assertEquals("wrong number of edges", 4L, summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
  public void testSummarizeOnVertexProperty() throws Exception {

    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] __VERTEX__ {city: "Leipzig", count: 2}
    // [2] __VERTEX__ {city: "Dresden", count: 3}
    // [5] __VERTEX__ {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
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

    // 5 summarized sna_edges:
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
  public void testSummarizeOnVertexPropertyWithAbsentValue() throws Exception {
    EPGraph inputGraph = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 2 summarized vertices:
    // [2] __VERTEX__ {city: "Dresden", count: 2}
    // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDDresden = 2L, vertexIDGraphProcessingForum = 10L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
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
          NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // 2 summarized sna_edges:
    // [16] Default -[__EDGE__]-> Dresden {count: 3}
    // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
    assertEquals("wrong number of edges", 2L, summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
  public void testSummarizeOnVertexAndEdgeProperty() throws Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, false, edgeGroupingKey, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] __VERTEX__ {city: "Leipzig", count: 2}
    // [2] __VERTEX__ {city: "Dresden", count: 3}
    // [5] __VERTEX__ {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
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

    // 6 summarized sna_edges:
    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
    // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
    // [0] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
  public void testSummarizeOnVertexAndEdgePropertyWithAbsentValues() throws
    Exception {
    EPGraph inputGraph = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, false, edgeGroupingKey, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 2 summarized vertices:
    // [2] __VERTEX__ {city: "Dresden", count: 2}
    // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDDresden = 2L, vertexIDGraphProcessingForum = 10L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
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
          NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // 3 summarized sna_edges:
    // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
    // [19] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
    assertEquals("wrong number of edges", 3L, summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
          NULL_VALUE, aggregatePropertyKey, 2, 1,
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

  @Test
  public void testSummarizeOnVertexLabel() throws Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String aggregatePropertyKey = "count";

    Summarization summarization = getSummarizationImpl(null, true, null, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] Person {count: 6}
    // [6] Tag {count: 3}
    // [9] Forum {count: 2}
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      System.out.println(v);
    }

    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L, vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 4 summarized sna_edges:
    // [0] Person -[__EDGE__]-> Person {count: 10}
    // [7] Person -[__EDGE__]-> Tag {count: 4}
    // [11] Forum -[__EDGE__]-> Tag {count: 4}
    // [15] Forum -[__EDGE__]-> Person {count: 6}
    long expectedEdgeCount = 4L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[__EDGE__]-> Person {count: 10}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, aggregatePropertyKey, 10, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Person -[__EDGE__]-> Tag {count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDTag, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[__EDGE__]-> Tag {count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDTag, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[__EDGE__]-> Person {count: 6}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDPerson, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexProperty() throws Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 5 summarized sna_edges:
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
  public void testSummarizeOnVertexLabelAndVertexPropertyWithAbsentValue()
    throws
    Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    // [6] Tag {city: "NULL", count: 3}
    // [9] Forum {city: "NULL", count: 2}
    assertEquals("wrong number of vertices", 5L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L,
      vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 11 summarized sna_edges:
    // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
    // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
    // [16] Forum -[__EDGE__]-> Dresden {count: 3}
    // [11] Forum -[__EDGE__]-> Tag {count: 4}
    // [15] Forum -[__EDGE__]-> Leipzig {count: 3}
    // [10] Berlin-[__EDGE__]-> Tag {count: 1}
    // [7] Dresden-[__EDGE__]-> Tag {count: 2}
    // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
    long expectedEdgeCount = 11L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
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
      } else if (e.getId().equals(16L)) {
        // [16] Forum -[__EDGE__]-> Dresden {count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDDresden, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[__EDGE__]-> Tag {count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDTag, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(10L)) {
        // [10] Berlin-[__EDGE__]-> Tag {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDTag, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Dresden-[__EDGE__]-> Tag {count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDTag, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(8L)) {
        // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDTag, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndEdgeProperty() throws Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(null, true, edgeGroupingKey, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 1 summarized vertex
    // [0] Person {count: 6}
    assertEquals("wrong number of vertices", 1L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());
      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 3 summarized sna_edges:
    // [0] Person -[__EDGE__]-> Person {since: 2014, count: 4}
    // [2] Person -[__EDGE__]-> Person {since: 2013, count: 3}
    // [21] Person -[__EDGE__]-> Person {since: 2015, count: 3}
    long expectedEdgeCount = 3L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[__EDGE__]-> Person {since: 2014, count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Person -[__EDGE__]-> Person {since: 2013, count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Person -[__EDGE__]-> Person {since: 2015, count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndEdgePropertyWithAbsentValue() throws
    Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(null, true, edgeGroupingKey, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] Person {count: 6}
    // [6] Tag {count: 3}
    // [9] Forum {count: 2}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L, vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 7 summarized sna_edges:
    // [0] Person -[__EDGE__]-> Person {since: 2014, count: 4}
    // [2] Person -[__EDGE__]-> Person {since: 2013, count: 3}
    // [21] Person -[__EDGE__]-> Person {since: 2015, count: 3}
    // [7] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
    // [11] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
    // [15] Forum -[__EDGE__]-> Person {since: __NULL, count: 5}
    // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
    long expectedEdgeCount = 7L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[__EDGE__]-> Person {since: 2014, count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Person -[__EDGE__]-> Person {since: 2013, count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Person -[__EDGE__]-> Person {since: 2015, count: 3}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDTag, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDTag, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[__EDGE__]-> Person {since: __NULL, count: 5}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDPerson, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 5,
          1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(16L)) {
        // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexAndEdgeProperty() throws
    Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, false);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 6 summarized sna_edges
    // [4] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
    // [3] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
    // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
    // [0] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
    // [2] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
    // [22] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
        testEdge(e, FlinkConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDDresden, edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabel() throws Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String aggregatePropertyKey = "count";

    Summarization summarization = getSummarizationImpl(null, true, null, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] Person {count: 6}
    // [6] Tag {count: 3}
    // [9] Forum {count: 2}
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      System.out.println(v);
    }

    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L, vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 5 summarized sna_edges:
    // [0] Person -[knows]-> Person {count: 10}
    // [7] Person -[hasInterest]-> Tag {count: 4}
    // [11] Forum -[hasTag]-> Tag {count: 4}
    // [15] Forum -[hasModerator]-> Person {count: 2}
    // [17] Forum -[hasMember]-> Person {count: 4}
    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[knows]-> Person {count: 10}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          aggregatePropertyKey, 10, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Person -[hasInterest]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[hasTag]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[hasModerator]-> Person {count: 2}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(17L)) {
        // [17] Forum -[hasMember]-> Person {count: 4}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDPerson,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndVertexProperty() throws
    Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 5 summarized sna_edges
    // [4] Dresden -[knows]-> Dresden {count: 2}
    // [3] Dresden -[knows]-> Leipzig {count: 3}
    // [0] Leipzig -[knows]-> Leipzig {count: 2}
    // [2] Leipzig -[knows]-> Dresden {count: 1}
    // [22] Berlin  -[knows]-> Dresden {count: 2}
    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin  -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void
  testSummarizeOnVertexAndEdgeLabelAndVertexPropertyWithAbsentValue() throws
    Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    // [6] Tag {city: "NULL", count: 3}
    // [9] Forum {city: "NULL", count: 2}
    assertEquals("wrong number of vertices", 5L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L,
      vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 13 summarized sna_edges:
    // [4] Dresden -[knows]-> Dresden {count: 2}
    // [3] Dresden -[knows]-> Leipzig {count: 3}
    // [0] Leipzig -[knows]-> Leipzig {count: 2}
    // [2] Leipzig -[knows]-> Dresden {count: 1}
    // [22] Berlin -[knows]-> Dresden {count: 2}
    // [16] Forum -[hasModerator]-> Dresden {count: 1}
    // [19] Forum -[hasMember]-> Dresden {count: 2}
    // [11] Forum -[hasTag]-> Tag {count: 4}
    // [15] Forum -[hasModerator]-> Leipzig {count: 1}
    // [17] Forum -[hasMember]-> Leipzig {count: 2}
    // [10] Berlin -[hasInterest]-> Tag {count: 1}
    // [7] Dresden -[hasInterest]-> Tag {count: 2}
    // [8] Leipzig -[hasInterest]-> Tag {count: 1}
    long expectedEdgeCount = 13L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[knows]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[knows]-> Leipzig {count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[knows]-> Leipzig {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[knows]-> Dresden {count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin -[knows]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(16L)) {
        // [16] Forum -[hasModerator]-> Dresden {count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(19L)) {
        // [19] Forum -[hasMember]-> Dresden {count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[hasTag]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[hasModerator]-> Leipzig {count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(17L)) {
        // [17] Forum -[hasMember]-> Leipzig {count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(10L)) {
        // [10] Berlin -[hasInterest]-> Tag {count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Dresden -[hasInterest]-> Tag {count: 2}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(8L)) {
        // [8] Leipzig -[hasInterest]-> Tag {count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDLeipzig, vertexIDTag,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndEdgeProperty() throws
    Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(null, true, edgeGroupingKey, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 1 summarized vertex
    // [0] Person {count: 6}
    assertEquals("wrong number of vertices", 1L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());
      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 3 summarized sna_edges:
    // [0] Person -[knows]-> Person {since: 2014, count: 4}
    // [2] Person -[knows]-> Person {since: 2013, count: 3}
    // [21] Person -[knows]-> Person {since: 2015, count: 3}
    long expectedEdgeCount = 3L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[knows]-> Person {since: 2014, count: 4}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Person -[knows]-> Person {since: 2013, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Person -[knows]-> Person {since: 2015, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndEdgePropertyWithAbsentValue
    () throws
    Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(null, true, edgeGroupingKey, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] Person {count: 6}
    // [6] Tag {count: 3}
    // [9] Forum {count: 2}
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      System.out.println(v);
    }

    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDPerson = 0L, vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDPerson)) {
        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 8 summarized sna_edges:
    // [0] Person -[knows]-> Person {since: 2014, count: 4}
    // [2] Person -[knows]-> Person {since: 2013, count: 3}
    // [21] Person -[knows]-> Person {since: 2015, count: 3}
    // [7] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
    // [11] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
    // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
    // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
    // [17] Forum -[hasMember]-> Person {since: __NULL, count: 4}
    long expectedEdgeCount = 8L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(0L)) {
        // [0] Person -[knows]-> Person {since: 2014, count: 4}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Person -[knows]-> Person {since: 2013, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Person -[knows]-> Person {since: 2015, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(16L)) {
        // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(17L)) {
        // [17] Forum -[hasMember]-> Person {since: __NULL, count: 4}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDPerson,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgeProperty() throws
    Exception {
    EPGraph inputGraph =
      graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
        .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 6 summarized sna_edges
    // [4] Dresden -[knows]-> Dresden {since: 2014, count: 2}
    // [3] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
    // [0] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
    // [22] Berlin -[knows]-> Dresden {since: 2015, count: 2}
    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[knows]-> Dresden {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin -[knows]-> Dresden {since: 2015, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void
  testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgePropertyWithAbsentValue()
    throws
    Exception {
    EPGraph inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, true);
    EPGraph summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 5 summarized vertices:
    // [0] Person {city: "Leipzig", count: 2}
    // [2] Person {city: "Dresden", count: 3}
    // [5] Person {city: "Berlin", count: 1}
    // [6] Tag {city: "NULL", count: 3}
    // [9] Forum {city: "NULL", count: 2}
    assertEquals("wrong number of vertices", 5L,
      summarizedGraph.getVertexCount());
    long vertexIDLeipzig = 0L, vertexIDDresden = 2L, vertexIDBerlin = 5L,
      vertexIDTag = 6L, vertexIDForum = 9L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (v.getId().equals(vertexIDLeipzig)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDTag)) {
        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDForum)) {
        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 14 summarized sna_edges:
    // [4] Dresden -[knows]-> Dresden {since: 2014, count: 2}
    // [3] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
    // [0] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
    // [22] Berlin -[knows]-> Dresden {since: 2015, count: 2}
    // [16] Forum -[hasModerator]-> Dresden {since: 2013, count: 1}
    // [19] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
    // [11] Forum -[hasTag]-> Tag {since: NULL, count: 4}
    // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
    // [17] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
    // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
    // [7] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
    // [8] Leipzig -[hasInterest]-> Tag {since: NULL, count: 1}
    long expectedEdgeCount = 14L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (e.getId().equals(4L)) {
        // [4] Dresden -[knows]-> Dresden {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(3L)) {
        // [3] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(21L)) {
        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(0L)) {
        // [0] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(2L)) {
        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(22L)) {
        // [22] Berlin -[knows]-> Dresden {since: 2015, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(16L)) {
        // [16] Forum -[hasModerator]-> Dresden {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(19L)) {
        // [19] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(11L)) {
        // [11] Forum -[hasTag]-> Tag {since: NULL, count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(15L)) {
        // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(17L)) {
        // [17] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(10L)) {
        // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(7L)) {
        // [7] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (e.getId().equals(8L)) {
        // [8] Leipzig -[hasInterest]-> Tag {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDLeipzig, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  private void testVertex(VertexData vertex, String expectedVertexLabel,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    testVertex(vertex, expectedVertexLabel, null, null, aggregatePropertyKey,
      expectedCountValue, expectedGraphCount, expectedGraphID);
  }

  private void testVertex(VertexData vertex, String expectedVertexLabel,
    String vertexGroupingKey, String expectedVertexGroupingValue,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    assertEquals("wrong vertex label", expectedVertexLabel, vertex.getLabel());
    if (vertexGroupingKey != null && expectedVertexGroupingValue != null) {
      assertEquals("wrong property value", expectedVertexGroupingValue,
        vertex.getProperty(vertexGroupingKey));
    }
    assertEquals("wrong vertex property", expectedCountValue,
      vertex.getProperty(aggregatePropertyKey));
    assertEquals("wrong number of graphs", expectedGraphCount,
      vertex.getGraphs().size());
    assertTrue("wrong graph id", vertex.getGraphs().contains(expectedGraphID));
  }

  private void testEdge(EdgeData edge, String expectedEdgeLabel,
    Long expectedSourceVertex, Long expectedTargetVertex,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    testEdge(edge, expectedEdgeLabel, expectedSourceVertex,
      expectedTargetVertex, null, null, aggregatePropertyKey,
      expectedCountValue, expectedGraphCount, expectedGraphID);
  }

  private void testEdge(EdgeData edge, String expectedEdgeLabel,
    Long expectedSourceVertex, Long expectedTargetVertex,
    String edgeGroupingKey, String expectedGroupingValue,
    String aggregatePropertyKey, Integer expectedCountValue,
    int expectedGraphCount, Long expectedGraphID) {
    assertEquals("wrong edge label", expectedEdgeLabel, edge.getLabel());
    assertEquals("wrong source vertex", expectedSourceVertex,
      edge.getSourceVertexId());
    assertEquals("wrong target vertex", expectedTargetVertex,
      edge.getTargetVertexId());
    assertEquals("wrong edge property", expectedCountValue,
      edge.getProperty(aggregatePropertyKey));
    assertEquals("wrong number of graphs", expectedGraphCount,
      edge.getGraphs().size());
    assertTrue("wrong graph id", edge.getGraphs().contains(expectedGraphID));

    if (edgeGroupingKey != null && expectedGroupingValue != null) {
      assertEquals("wrong group value", expectedGroupingValue,
        edge.getProperty(edgeGroupingKey));
    }
  }
}
