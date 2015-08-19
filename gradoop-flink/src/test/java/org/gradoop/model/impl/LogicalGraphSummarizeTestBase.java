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

import com.google.common.collect.Lists;
import org.gradoop.GConstants;
import org.gradoop.model.EdgeData;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.impl.operators.Summarization;
import org.junit.Test;

import java.util.List;

import static org.gradoop.GradoopTestBaseUtils.*;
import static org.gradoop.model.impl.operators.Summarization.NULL_VALUE;
import static org.junit.Assert.*;

public abstract class LogicalGraphSummarizeTestBase extends FlinkTestBase {

  public LogicalGraphSummarizeTestBase(TestExecutionMode mode) {
    super(mode);
  }

  public abstract Summarization<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel);

  @Test
  public void testSummarizeOnVertexPropertySymmetricGraph() throws Exception {

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(2L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 2 summarized vertices:
    // [0] __VERTEX__ {city: "Leipzig", count: 2}
    // [2] __VERTEX__ {city: "Dresden", count: 2}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());

    Long vertexIdLeipzig = 0L;
    Long vertexIdDresden = 2L;

    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (vertexIdLeipzig.equals(v.getId())) {
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (vertexIdDresden.equals(v.getId())) {
        vertexIdDresden = v.getId();
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 4 summarized sna_edges:
    // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L);
    // [3] Dresden -[__EDGE__]-> Leipzig {count: 1}
    List<Long> dresdenToLeipzigEdgeIds = Lists.newArrayList(3L);
    // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);

    assertEquals("wrong number of edges", 4L, summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
          vertexIdLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
          vertexIdDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
        // [3] Dresden -[__EDGE__]-> Leipzig {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
          vertexIdLeipzig, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
          vertexIdDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexProperty() throws Exception {

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
    assertNotNull("summarized graph must not be null", summarizedGraph);

    // 3 summarized vertices:
    // [0] __VERTEX__ {city: "Leipzig", count: 2}
    // [2] __VERTEX__ {city: "Dresden", count: 3}
    // [5] __VERTEX__ {city: "Berlin", count: 1}
    assertEquals("wrong number of vertices", 3L,
      summarizedGraph.getVertexCount());

    Long vertexIdLeipzig = 0L;
    Long vertexIdDresden = 2L;
    Long vertexIdBerlin = 5L;
    for (VertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());

      if (vertexIdLeipzig.equals(v.getId())) {
        vertexIdLeipzig = v.getId();
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (vertexIdDresden.equals(v.getId())) {
        vertexIdDresden = v.getId();
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (vertexIdBerlin.equals(v.getId())) {
        vertexIdBerlin = v.getId();
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Berlin", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 5 summarized sna_edges:
    // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
    List<Long> dresdenToLeipzigEdgeIds = Lists.newArrayList(3L, 6L, 21L);
    // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L);
    // [22-23] Berlin -[__EDGE__]-> Dresden {count: 2}
    List<Long> berlinToDresdenEdgeIds = Lists.newArrayList(22L, 23L);

    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
          vertexIdDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
          vertexIdLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
          vertexIdLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
          vertexIdDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
        // [22-23] Berlin -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdBerlin,
          vertexIdDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexPropertyWithAbsentValue() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, false, null, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDGraphProcessingForum)) {
        // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // 2 summarized sna_edges:
    // [16,19,20] Default -[__EDGE__]-> Dresden {count: 3}
    List<Long> defaultToDresdenEdgeIds = Lists.newArrayList(16L, 19L, 20L);
    // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L);

    assertEquals("wrong number of edges", 2L, summarizedGraph.getEdgeCount());


    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (defaultToDresdenEdgeIds.contains(e.getId())) {
        // [16,19,20] Default -[__EDGE__]-> Dresden {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDGraphProcessingForum,
          vertexIDDresden, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeProperty() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, false, edgeGroupingKey, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Leipzig", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDDresden)) {
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDBerlin)) {
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Berlin", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected vertex", false);
      }
    }

    // 6 summarized sna_edges:
    // [4-5] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
    List<Long> dresdenToLeipzigEdgeIds1 = Lists.newArrayList(3L, 6L);
    // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
    List<Long> dresdenToLeipzigEdgeIds2 = Lists.newArrayList(21L);
    // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L);
    // [22-23] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
    List<Long> berlinToDresdenEdgeIds = Lists.newArrayList(22L, 23L);

    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4-5] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds1.contains(e.getId())) {
        // [3,6] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds2.contains(e.getId())) {
        // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
        // [22-23] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, false, edgeGroupingKey, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          "Dresden", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (v.getId().equals(vertexIDGraphProcessingForum)) {
        // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      }
    }

    // 3 summarized sna_edges:
    // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
    List<Long> defaultToDresdenEdgeIds1 = Lists.newArrayList(16L);
    // [19-20] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
    List<Long> defaultToDresdenEdgeIds2 = Lists.newArrayList(19L, 20L);
    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L);

    assertEquals("wrong number of edges", 3L, summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (defaultToDresdenEdgeIds1.contains(e.getId())) {
        // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDGraphProcessingForum,
          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (defaultToDresdenEdgeIds2.contains(e.getId())) {
        // [19-20] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDGraphProcessingForum,
          vertexIDDresden, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2,
          1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabel() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, null, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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

    // 4 summarized sna_edges:
    // [0,1,2,3,4,5,6,21,22,23] Person -[__EDGE__]-> Person {count: 10}
    List<Long> personToPersonEdgeIds =
      Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 21L, 22L, 23L);
    // [7,8,9,10] Person -[__EDGE__]-> Tag {count: 4}
    List<Long> personToTagEdgeIds = Lists.newArrayList(7L, 8L, 9L, 10L);
    // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
    List<Long> forumToTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15,16,17,18,19,20] Forum -[__EDGE__]-> Person {count: 6}
    List<Long> forumToPersonEdgeIds =
      Lists.newArrayList(15L, 16L, 17L, 18L, 19L, 20L);

    long expectedEdgeCount = 4L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (personToPersonEdgeIds.contains(e.getId())) {
        // [0,1,3,4,5,6,21,22,23] Person -[__EDGE__]-> Person {count: 10}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, aggregatePropertyKey, 10, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToTagEdgeIds.contains(e.getId())) {
        // [7,8,9,10] Person -[__EDGE__]-> Tag {count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToPersonEdgeIds.contains(e.getId())) {
        // [15,16,17,18,19,20] Forum -[__EDGE__]-> Person {count: 6}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDPerson, aggregatePropertyKey, 6, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndVertexProperty() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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

    // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
    List<Long> dresdenToLeipzigEdgeIds = Lists.newArrayList(3L, 6L, 21L);
    // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L);
    // [22-23] Berlin  -[__EDGE__]-> Dresden {count: 2}
    List<Long> berlinToDresdenEdgeIds = Lists.newArrayList(22L, 23L);

    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
        // [22-23] Berlin  -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, true, null, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
    List<Long> dresdenToLeipzigEdgeIds = Lists.newArrayList(3L, 6L, 21L);
    // [0,1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L);
    // [22,23] Berlin  -[__EDGE__]-> Dresden {count: 2}
    List<Long> berlinToDresdenEdgeIds = Lists.newArrayList(22L, 23L);
    // [16,19,20] Forum -[__EDGE__]-> Dresden {count: 3}
    List<Long> forumToDresdenEdgeIds = Lists.newArrayList(16L, 19L, 20L);
    // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
    List<Long> forumToTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15,17,18] Forum -[__EDGE__]-> Leipzig {count: 3}
    List<Long> forumToLeipzigEdgeIds = Lists.newArrayList(15L, 17L, 18L);
    // [10] Berlin-[__EDGE__]-> Tag {count: 1}
    List<Long> berlinToTagEdgeIds = Lists.newArrayList(10L);
    // [7,9] Dresden-[__EDGE__]-> Tag {count: 2}
    List<Long> dresdenToTagEdgeIds = Lists.newArrayList(7L, 9L);
    // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
    List<Long> leipzigToTagEdgeIds = Lists.newArrayList(8L);

    long expectedEdgeCount = 11L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
        // [22,23] Berlin  -[__EDGE__]-> Dresden {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDDresden, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToDresdenEdgeIds.contains(e.getId())) {
        // [16,19,20] Forum -[__EDGE__]-> Dresden {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDDresden, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToLeipzigEdgeIds.contains(e.getId())) {
        // [15,17,18] Forum -[__EDGE__]-> Leipzig {count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToTagEdgeIds.contains(e.getId())) {
        // [10] Berlin-[__EDGE__]-> Tag {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin, vertexIDTag,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToTagEdgeIds.contains(e.getId())) {
        // [7,9] Dresden-[__EDGE__]-> Tag {count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden, vertexIDTag,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToTagEdgeIds.contains(e.getId())) {
        // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig, vertexIDTag,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexLabelAndEdgeProperty() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, edgeGroupingKey, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
    List<Long> personToPersonEdgeIds1 = Lists.newArrayList(0L, 1L, 4L, 5L);
    // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
    List<Long> personToPersonEdgeIds2 = Lists.newArrayList(2L, 3L, 6L);
    // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
    List<Long> personToPersonEdgeIds3 = Lists.newArrayList(21L, 22L, 23L);
    long expectedEdgeCount = 3L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (personToPersonEdgeIds1.contains(e.getId())) {
        // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToPersonEdgeIds2.contains(e.getId())) {
        // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToPersonEdgeIds3.contains(e.getId())) {
        // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, edgeGroupingKey, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
    List<Long> personToPersonEdgeIds1 = Lists.newArrayList(0L, 1L, 4L, 5L);
    // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
    List<Long> personToPersonEdgeIds2 = Lists.newArrayList(2L, 3L, 6L);
    // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
    List<Long> personToPersonEdgeIds3 = Lists.newArrayList(21L, 22L, 23L);
    // [7,8,9,10] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
    List<Long> personToTagEdgeIds = Lists.newArrayList(7L, 8L, 9L, 10L);
    // [11,12,13,14] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
    List<Long> forumToTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15,17,18,19,20] Forum -[__EDGE__]-> Person {since: __NULL, count: 5}
    List<Long> forumToPersonEdgeIds1 =
      Lists.newArrayList(15L, 17L, 18L, 19L, 20L);
    // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
    List<Long> forumToPersonEdgeIds2 = Lists.newArrayList(16L);

    long expectedEdgeCount = 7L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (personToPersonEdgeIds1.contains(e.getId())) {
        // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToPersonEdgeIds2.contains(e.getId())) {
        // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToPersonEdgeIds3.contains(e.getId())) {
        // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
          vertexIDPerson, edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personToTagEdgeIds.contains(e.getId())) {
        // [7,8,9,10] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToPersonEdgeIds1.contains(e.getId())) {
        // [15,17,18,19,20] Forum -[__EDGE__]-> Person {since: __NULL, count: 5}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
          vertexIDPerson, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 5,
          1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumToPersonEdgeIds2.contains(e.getId())) {
        // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, false);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
    List<Long> dresdenToDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
    List<Long> dresdenToLeipzigEdgeIds1 = Lists.newArrayList(3L, 6L);
    // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
    List<Long> dresdenToLeipzigEdgeIds2 = Lists.newArrayList(21L);
    // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
    List<Long> leipzigToLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2,3,4] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
    List<Long> leipzigToDresdenEdgeIds = Lists.newArrayList(2L, 3L, 4L);
    // [22,23] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
    List<Long> berlinToDresdenEdgeIds = Lists.newArrayList(22L, 23L);
    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds1.contains(e.getId())) {
        // [3,6] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenToLeipzigEdgeIds2.contains(e.getId())) {
        // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
          vertexIDLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
        // [2,3,4] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
        // [22,23] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
          vertexIDDresden, edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else {
        assertTrue("unexpected edge: " + e.getId(), false);
      }
    }
  }

  @Test
  public void testSummarizeOnVertexAndEdgeLabel() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, null, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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

    // 5 summarized sna_edges:
    // [0,1,2,3,4,5,6,21,22,23] Person -[knows]-> Person {count: 10}
    List<Long> knowsEdgeIds =
      Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 21L, 22L, 23L);
    assertEquals(10, knowsEdgeIds.size());
    // [7,8,9,10] Person -[hasInterest]-> Tag {count: 4}
    List<Long> hasInterestEdgeIds = Lists.newArrayList(7L, 8L, 9L, 10L);
    assertEquals(4, hasInterestEdgeIds.size());
    // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
    List<Long> hasTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    assertEquals(4, hasTagEdgeIds.size());
    // [15,16] Forum -[hasModerator]-> Person {count: 2}
    List<Long> hasModeratorEdgeIds = Lists.newArrayList(15L, 16L);
    assertEquals(2, hasModeratorEdgeIds.size());
    // [17,18,19,20] Forum -[hasMember]-> Person {count: 4}
    List<Long> hasMemberEdgeIds = Lists.newArrayList(17L, 18L, 19L, 20L);
    assertEquals(4, hasMemberEdgeIds.size());

    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (knowsEdgeIds.contains(e.getId())) {
        // [0,1,2,3,4,5,6,21,22,23] Person -[knows]-> Person {count: 10}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          aggregatePropertyKey, 10, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (hasInterestEdgeIds.contains(e.getId())) {
        // [7,8,9,10] Person -[hasInterest]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (hasTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (hasModeratorEdgeIds.contains(e.getId())) {
        // [15,16] Forum -[hasModerator]-> Person {count: 2}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (hasMemberEdgeIds.contains(e.getId())) {
        // [17,18,19,20] Forum -[hasMember]-> Person {count: 4}
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(vertexGroupingKey, true, null, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[knows]-> Dresden {count: 2}
    List<Long> dresdenKnowsDresdenEdgeEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
    List<Long> dresdenKnowsLeipzig = Lists.newArrayList(3L, 6L, 21L);
    // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
    List<Long> leipzigKnowsLeipzig = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[knows]-> Dresden {count: 1}
    List<Long> leipzigKnowsDresden = Lists.newArrayList(2L);
    // [22,23] Berlin  -[knows]-> Dresden {count: 2}
    List<Long> berlinKnowsDresden = Lists.newArrayList(22L, 23L);

    long expectedEdgeCount = 5L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenKnowsDresdenEdgeEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[knows]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzig.contains(e.getId())) {
        // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsLeipzig.contains(e.getId())) {
        // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsDresden.contains(e.getId())) {
        // [2] Leipzig -[knows]-> Dresden {count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinKnowsDresden.contains(e.getId())) {
        // [22,23] Berlin -[knows]-> Dresden {count: 2}
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(vertexGroupingKey, true, null, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[knows]-> Dresden {count: 2}
    List<Long> dresdenKnowsDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
    List<Long> dresdenKnowsLeipzigEdgeIds = Lists.newArrayList(3L, 6L, 21L);
    // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
    List<Long> leipzigKnowsLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[knows]-> Dresden {count: 1}
    List<Long> leipzigKnowsDresdenEdgeIds = Lists.newArrayList(2L);
    // [22,23] Berlin -[knows]-> Dresden {count: 2}
    List<Long> berlinKnowsDresdenEdgeIds = Lists.newArrayList(22L, 23L);
    // [16] Forum -[hasModerator]-> Dresden {count: 1}
    List<Long> forumHasModeratorDresdenEdgeIds = Lists.newArrayList(16L);
    // [19,20] Forum -[hasMember]-> Dresden {count: 2}
    List<Long> forumHasMemberDresdenEdgeIds = Lists.newArrayList(19L, 20L);
    // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
    List<Long> forumHasTagTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15] Forum -[hasModerator]-> Leipzig {count: 1}
    List<Long> forumHasModeratorLeipzigEdgeIds = Lists.newArrayList(15L);
    // [17,18] Forum -[hasMember]-> Leipzig {count: 2}
    List<Long> forumHasMemberLeipzigEdgeIds = Lists.newArrayList(17L, 18L);
    // [10] Berlin -[hasInterest]-> Tag {count: 1}
    List<Long> berlinHasInterestTagEdgeIds = Lists.newArrayList(10L);
    // [7,9] Dresden -[hasInterest]-> Tag {count: 2}
    List<Long> dresdenHasInterestTagEdgeIds = Lists.newArrayList(7L, 9L);
    // [8] Leipzig -[hasInterest]-> Tag {count: 1}
    List<Long> leipzigHasInterestTagEdgeIds = Lists.newArrayList(8L);
    long expectedEdgeCount = 13L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[knows]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzigEdgeIds.contains(e.getId())) {
        // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[knows]-> Dresden {count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
        // [22,23] Berlin -[knows]-> Dresden {count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorDresdenEdgeIds.contains(e.getId())) {
        // [16] Forum -[hasModerator]-> Dresden {count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasMemberDresdenEdgeIds.contains(e.getId())) {
        // [19,20] Forum -[hasMember]-> Dresden {count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorLeipzigEdgeIds.contains(e.getId())) {
        // [15] Forum -[hasModerator]-> Leipzig {count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasMemberLeipzigEdgeIds.contains(e.getId())) {
        // [17,18] Forum -[hasMember]-> Leipzig {count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinHasInterestTagEdgeIds.contains(e.getId())) {
        // [10] Berlin -[hasInterest]-> Tag {count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenHasInterestTagEdgeIds.contains(e.getId())) {
        // [7,9] Dresden -[hasInterest]-> Tag {count: 2}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigHasInterestTagEdgeIds.contains(e.getId())) {
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, edgeGroupingKey, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
    List<Long> personKnowsPersonEdgeIds1 = Lists.newArrayList(0L, 1L, 4L, 5L);
    // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
    List<Long> personKnowsPersonEdgeIds2 = Lists.newArrayList(2L, 3L, 6L);
    // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
    List<Long> personKnowsPersonEdgeIds3 = Lists.newArrayList(21L, 22L, 23L);
    long expectedEdgeCount = 3L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (personKnowsPersonEdgeIds1.contains(e.getId())) {
        // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personKnowsPersonEdgeIds2.contains(e.getId())) {
        // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personKnowsPersonEdgeIds3.contains(e.getId())) {
        // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization = getSummarizationImpl(null, true, edgeGroupingKey, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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

    // 8 summarized sna_edges:
    // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
    List<Long> personKnowsPersonEdgeIds1 = Lists.newArrayList(0L, 1L, 4L, 5L);
    // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
    List<Long> personKnowsPersonEdgeIds2 = Lists.newArrayList(2L, 3L, 6L);
    // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
    List<Long> personKnowsPersonEdgeIds3 = Lists.newArrayList(21L, 22L, 23L);
    // [7,8,9,10] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
    List<Long> personHasInterestTagEdgeIds =
      Lists.newArrayList(7L, 8L, 9L, 10L);
    // [11,12,13,14] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
    List<Long> forumHasTagTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
    List<Long> forumHasModeratorPersonEdgeIds1 = Lists.newArrayList(15L);
    // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
    List<Long> forumHasModeratorPersonEdgeIds2 = Lists.newArrayList(16L);
    // [17,18,19,20] Forum -[hasMember]-> Person {since: __NULL, count: 4}
    List<Long> forumHasMemberPersonEdgeIds =
      Lists.newArrayList(17L, 18L, 19L, 20L);
    long expectedEdgeCount = 8L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (personKnowsPersonEdgeIds1.contains(e.getId())) {
        // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personKnowsPersonEdgeIds2.contains(e.getId())) {
        // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personKnowsPersonEdgeIds3.contains(e.getId())) {
        // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
          edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (personHasInterestTagEdgeIds.contains(e.getId())) {
        // [7,8,9,10] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorPersonEdgeIds1.contains(e.getId())) {
        // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorPersonEdgeIds2.contains(e.getId())) {
        // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasMemberPersonEdgeIds.contains(e.getId())) {
        // [17,18,19,20] Forum -[hasMember]-> Person {since: __NULL, count: 4}
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getGraph(0L).combine(graphStore.getGraph(1L))
      .combine(graphStore.getGraph(2L));

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
    List<Long> dresdenKnowsDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
    List<Long> dresdenKnowsLeipzigEdgeIds1 = Lists.newArrayList(3L, 6L);
    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
    List<Long> dresdenKnowsLeipzigEdgeIds2 = Lists.newArrayList(21L);
    // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
    List<Long> leipzigKnowsLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
    List<Long> leipzigKnowsDresdenEdgeIds = Lists.newArrayList(2L);
    // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
    List<Long> berlinKnowsDresdenEdgeIds = Lists.newArrayList(22L, 23L);

    long expectedEdgeCount = 6L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzigEdgeIds1.contains(e.getId())) {
        // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzigEdgeIds2.contains(e.getId())) {
        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
        // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
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
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = graphStore.getDatabaseGraph();

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "since";
    final String aggregatePropertyKey = "count";

    Summarization<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarization =
      getSummarizationImpl(vertexGroupingKey, true, edgeGroupingKey, true);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      summarizedGraph = summarization.execute(inputGraph);
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
    // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
    List<Long> dresdenKnowsDresdenEdgeIds = Lists.newArrayList(4L, 5L);
    // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
    List<Long> dresdenKnowsLeipzigEdgeIds1 = Lists.newArrayList(3L, 6L);
    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
    List<Long> dresdenKnowsLeipzigEdgeIds2 = Lists.newArrayList(21L);
    // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
    List<Long> leipzigKnowsLeipzigEdgeIds = Lists.newArrayList(0L, 1L);
    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
    List<Long> leipzigKnowsDresdenEdgeIds = Lists.newArrayList(2L);
    // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
    List<Long> berlinKnowsDresdenEdgeIds = Lists.newArrayList(22L, 23L);
    // [16] Forum -[hasModerator]-> Dresden {since: 2013, count: 1}
    List<Long> forumHasModeratorDresdenEdgeIds = Lists.newArrayList(16L);
    // [19,20] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
    List<Long> forumHasMemberDresdenEdgeIds = Lists.newArrayList(19L, 20L);
    // [11,12,13,14] Forum -[hasTag]-> Tag {since: NULL, count: 4}
    List<Long> forumHasTagTagEdgeIds = Lists.newArrayList(11L, 12L, 13L, 14L);
    // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
    List<Long> forumHasModeratorLeipzigEdgeIds = Lists.newArrayList(15L);
    // [17,18] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
    List<Long> forumHasMemberLeipzigEdgeIds = Lists.newArrayList(17L, 18L);
    // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
    List<Long> berlinHasInterestTagEdgeIds = Lists.newArrayList(10L);
    // [7,9] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
    List<Long> dresdenHasInterestTagEdgeIds = Lists.newArrayList(7L, 9L);
    // [8] Leipzig -[hasInterest]-> Tag {since: NULL, count: 1}
    List<Long> leipzigHasInterestTagEdgeIds = Lists.newArrayList(8L);

    long expectedEdgeCount = 14L;
    assertEquals("wrong number of edges", expectedEdgeCount,
      summarizedGraph.getEdgeCount());

    for (EdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());

      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
        // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzigEdgeIds1.contains(e.getId())) {
        // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenKnowsLeipzigEdgeIds2.contains(e.getId())) {
        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
        // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
        // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
          edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorDresdenEdgeIds.contains(e.getId())) {
        // [16] Forum -[hasModerator]-> Dresden {since: 2013, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasMemberDresdenEdgeIds.contains(e.getId())) {
        // [19,20] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
        // [11,12,13,14] Forum -[hasTag]-> Tag {since: NULL, count: 4}
        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
          NULL_VALUE, aggregatePropertyKey, 4, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasModeratorLeipzigEdgeIds.contains(e.getId())) {
        // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (forumHasMemberLeipzigEdgeIds.contains(e.getId())) {
        // [17,18] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (berlinHasInterestTagEdgeIds.contains(e.getId())) {
        // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (dresdenHasInterestTagEdgeIds.contains(e.getId())) {
        // [7,9] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
          FlinkConstants.SUMMARIZE_GRAPH_ID);
      } else if (leipzigHasInterestTagEdgeIds.contains(e.getId())) {
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
      vertex.getGraphCount());
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
