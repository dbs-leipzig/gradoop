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

package org.gradoop.model.impl.operators.logicalgraph.unary.summarization;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public abstract class EPGMGraphSummarizeTestBase extends FlinkTestBase {

  public EPGMGraphSummarizeTestBase(TestExecutionMode mode) {
    super(mode);
  }

  public abstract Summarization<VertexPojo, EdgePojo, GraphHeadPojo> getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel);

  @Test
  public void testSummarizeOnVertexPropertySymmetricGraph() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    loader.appendToDatabaseFromString("expected[" +
      "(leipzig {city=\"Leipzig\",count=2});" +
      "(dresden {city=\"Dresden\",count=2});" +
      "(leipzig)-[{count=2}]->(leipzig);" +
      "(leipzig)-[{count=1}]->(dresden);" +
      "(dresden)-[{count=2}]->(dresden);" +
      "(dresden)-[{count=1}]->(leipzig)]");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g2 = loader
      .getLogicalGraphByVariable("g2");
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("expected");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> result =
      new SummarizationRunner(g2, "city", false, null, false).run();

    assertTrue(result.equalsByElementDataCollected(expected));
  }

//  @Test
//  public void testSummarizeOnVertexProperty() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, false, null, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1] __VERTEX__ {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] __VERTEX__ {city: "Dresden", count: 3}
//    GradoopIds vertexIdsDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] __VERTEX__ {city: "Berlin", count: 1}
//    GradoopIds vertexIdsBerlin = GradoopIds.fromLongs(5L);
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//
//    GradoopId vertexIdLeipzig = null;
//    GradoopId vertexIdDresden = null;
//    GradoopId vertexIdBerlin = null;
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsLeipzig.contains(v.getId())) {
//        vertexIdLeipzig = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Leipzig", aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsDresden.contains(v.getId())) {
//        vertexIdDresden = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Dresden", aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsBerlin.contains(v.getId())) {
//        vertexIdBerlin = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Berlin", aggregatePropertyKey, 1L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 5 summarized sna_edges:
//    // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//    GradoopIds dresdenToLeipzigEdgeIds = GradoopIds.fromLongs(3L, 6L, 21L);
//    // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//    GradoopIds leipzigToLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//    GradoopIds leipzigToDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22-23] Berlin -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds berlinToDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//
//    long expectedEdgeCount = 5L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4-5] Dresden -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
//        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdLeipzig, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
//        // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
//          vertexIdLeipzig, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
//          vertexIdDresden, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
//        // [22-23] Berlin -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdBerlin,
//          vertexIdDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexPropertyWithAbsentValue() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getGraph(GradoopId.fromLong(3L));
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, false, null, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 2 summarized vertices:
//    // [2,3] __VERTEX__ {city: "Dresden", count: 2}
//    GradoopIds vertexIdsDresden = GradoopIds.fromLongs(2L, 3L);
//    // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
//    GradoopIds vertexIdsDefault = GradoopIds.fromLongs(10L);
//    assertEquals("wrong number of vertices", 2L, vertices.size());
//    GradoopId vertexIdDresden = GradoopId.fromLong(0L);
//    GradoopId vertexIdDefault = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsDresden.contains(v.getId())) {
//        // [2,3] __VERTEX__ {city: "Dresden", count: 2}
//        vertexIdDresden = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Dresden", aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsDefault.contains(v.getId())) {
//        // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
//        vertexIdDefault = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          NULL_VALUE, aggregatePropertyKey, 1L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      }
//    }
//
//    // 2 summarized sna_edges:
//    // [16,19,20] Default -[__EDGE__]-> Dresden {count: 3}
//    GradoopIds defaultToDresdenEdgeIds = GradoopIds.fromLongs(16L, 19L, 20L);
//    // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L);
//
//    assertEquals("wrong number of edges", 2L, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (defaultToDresdenEdgeIds.contains(e.getId())) {
//        // [16,19,20] Default -[__EDGE__]-> Dresden {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDefault,
//          vertexIdDresden, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4] Dresden -[__EDGE__]-> Dresden {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdDresden, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeProperty() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, false,
//        edgeGroupingKey, false).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1] __VERTEX__ {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] __VERTEX__ {city: "Dresden", count: 3}
//    GradoopIds vertexIdsDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] __VERTEX__ {city: "Berlin", count: 1}
//    GradoopIds vertexIdsBerlin = GradoopIds.fromLongs(5L);
//
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIdLeipzig = GradoopId.fromLong(0L);
//    GradoopId vertexIdDresden = GradoopId.fromLong(0L);
//    GradoopId vertexIdBerlin = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsLeipzig.contains(v.getId())) {
//        // [0,1] __VERTEX__ {city: "Leipzig", count: 2}
//        vertexIdLeipzig = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Leipzig", aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsDresden.contains(v.getId())) {
//        // [2,3,4] __VERTEX__ {city: "Dresden", count: 3}
//        vertexIdDresden = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Dresden", aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsBerlin.contains(v.getId())) {
//        // [5] __VERTEX__ {city: "Berlin", count: 1}
//        vertexIdBerlin = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Berlin", aggregatePropertyKey, 1L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 6 summarized sna_edges:
//    // [4-5] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
//    GradoopIds dresdenToLeipzigEdgeIds1 = GradoopIds.fromLongs(3L, 6L);
//    // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
//    GradoopIds dresdenToLeipzigEdgeIds2 = GradoopIds.fromLongs(21L);
//    // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
//    GradoopIds leipzigToLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
//    GradoopIds leipzigToDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22-23] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
//    GradoopIds berlinToDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//
//    long expectedEdgeCount = 6L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4-5] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds1.contains(e.getId())) {
//        // [3,6] Dresden -[__EDGE__]-> Leipzig {since: 2013, count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds2.contains(e.getId())) {
//        // [21] Dresden -[__EDGE__]-> Leipzig {since: 2015, count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: 2014, count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
//          vertexIdLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[__EDGE__]-> Dresden {since: 2013, count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdLeipzig,
//          vertexIdDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
//        // [22-23] Berlin  -[__EDGE__]-> Dresden {since: 2015, count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdBerlin,
//          vertexIdDresden, edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgePropertyWithAbsentValues() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getGraph(GradoopId.fromLong(3L));
//
//    final String vertexGroupingKey = "city";
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, false,
//        edgeGroupingKey, false).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 2 summarized vertices:
//    // [2,3] __VERTEX__ {city: "Dresden", count: 2}
//    GradoopIds vertexIdsDresden = GradoopIds.fromLongs(2L, 3L);
//    // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
//    GradoopIds vertexIdsDefault = GradoopIds.fromLongs(10L);
//
//    assertEquals("wrong number of vertices", 2L, vertices.size());
//    GradoopId vertexIdDresden = GradoopId.fromLong(0L);
//    GradoopId vertexIdDefault = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsDresden.contains(v.getId())) {
//        // [2,3] __VERTEX__ {city: "Dresden", count: 2}
//        vertexIdDresden = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          "Dresden", aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsDefault.contains(v.getId())) {
//        // [10] __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
//        vertexIdDefault = v.getId();
//        testVertex(v, GConstants.DEFAULT_VERTEX_LABEL, vertexGroupingKey,
//          NULL_VALUE, aggregatePropertyKey, 1L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      }
//    }
//
//    // 3 summarized sna_edges:
//    // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
//    GradoopIds defaultToDresdenEdgeIds1 = GradoopIds.fromLongs(16L);
//    // [19-20] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
//    GradoopIds defaultToDresdenEdgeIds2 = GradoopIds.fromLongs(19L, 20L);
//    // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L);
//
//    assertEquals("wrong number of edges", 3L, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (defaultToDresdenEdgeIds1.contains(e.getId())) {
//        // [16] Default -[__EDGE__]-> Dresden {since: 2013, count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDefault,
//          vertexIdDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (defaultToDresdenEdgeIds2.contains(e.getId())) {
//        // [19-20] Default -[__EDGE__]-> Dresden {since: NULL, count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDefault,
//          vertexIdDresden, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2,
//          1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4] Dresden -[__EDGE__]-> Dresden {since: 2014, count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdDresden,
//          vertexIdDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexLabel() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, null, false).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//    // [6,7,8] Tag {count: 3}
//    GradoopIds vertexIdsTag = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {count: 2}
//    GradoopIds vertexIdsForum = GradoopIds.fromLongs(9L, 10L);
//
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIdPerson = GradoopId.fromLong(0L);
//    GradoopId vertexIdTag = GradoopId.fromLong(0L);
//    GradoopId vertexIdForum = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPerson.contains(v.getId())) {
//        // [0,1,2,3,4,5] Person {count: 6}
//        vertexIdPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTag.contains(v.getId())) {
//        // [6,7,8] Tag {count: 3}
//        vertexIdTag = v.getId();
//        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForum.contains(v.getId())) {
//        // [9,10] Forum {count: 2}
//        vertexIdForum = v.getId();
//        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 4 summarized sna_edges:
//    // [0,1,2,3,4,5,6,21,22,23] Person -[__EDGE__]-> Person {count: 10}
//    GradoopIds personToPersonEdgeIds =
//      GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L, 6L, 21L, 22L, 23L);
//    // [7,8,9,10] Person -[__EDGE__]-> Tag {count: 4}
//    GradoopIds personToTagEdgeIds = GradoopIds.fromLongs(7L, 8L, 9L, 10L);
//    // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
//    GradoopIds forumToTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15,16,17,18,19,20] Forum -[__EDGE__]-> Person {count: 6}
//    GradoopIds forumToPersonEdgeIds =
//      GradoopIds.fromLongs(15L, 16L, 17L, 18L, 19L, 20L);
//
//    long expectedEdgeCount = 4L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (personToPersonEdgeIds.contains(e.getId())) {
//        // [0,1,3,4,5,6,21,22,23] Person -[__EDGE__]-> Person {count: 10}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdPerson,
//          vertexIdPerson, aggregatePropertyKey, 10, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToTagEdgeIds.contains(e.getId())) {
//        // [7,8,9,10] Person -[__EDGE__]-> Tag {count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdPerson, vertexIdTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdForum, vertexIdTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToPersonEdgeIds.contains(e.getId())) {
//        // [15,16,17,18,19,20] Forum -[__EDGE__]-> Person {count: 6}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIdForum,
//          vertexIdPerson, aggregatePropertyKey, 6, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexLabelAndVertexProperty() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true, null, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L);
//    GradoopId vertexIDDresden = GradoopId.fromLong(0L);
//    GradoopId vertexIDBerlin = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 5 summarized sna_edges:
//
//    // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//    GradoopIds dresdenToLeipzigEdgeIds = GradoopIds.fromLongs(3L, 6L, 21L);
//    // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//    GradoopIds leipzigToLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//    GradoopIds leipzigToDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22-23] Berlin  -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds berlinToDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//
//    long expectedEdgeCount = 5L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
//        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
//        // [0-1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDDresden, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
//        // [22-23] Berlin  -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
//          vertexIDDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexLabelAndVertexPropertyWithAbsentValue()
//    throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true, null, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 5 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    // [6,7,8] Tag {city: "NULL", count: 3}
//    GradoopIds vertexIdsTagNull = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {city: "NULL", count: 2}
//    GradoopIds vertexIdsForumNull = GradoopIds.fromLongs(9L, 10L);
//
//    assertEquals("wrong number of vertices", 5L, vertices.size());
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L);
//    GradoopId vertexIDDresden = GradoopId.fromLong(0L);
//    GradoopId vertexIDBerlin = GradoopId.fromLong(0L);
//    GradoopId vertexIDTag = GradoopId.fromLong(0L);
//    GradoopId vertexIDForum = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON,  vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTagNull.contains(v.getId())) {
//        // [6,7,8] Tag {city: "NULL", count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForumNull.contains(v.getId())) {
//        // [9,10] Forum {city: "NULL", count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 11 summarized sna_edges:
//    // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//    GradoopIds dresdenToLeipzigEdgeIds = GradoopIds.fromLongs(3L, 6L, 21L);
//    // [0,1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//    GradoopIds leipzigToLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//    GradoopIds leipzigToDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22,23] Berlin  -[__EDGE__]-> Dresden {count: 2}
//    GradoopIds berlinToDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//    // [16,19,20] Forum -[__EDGE__]-> Dresden {count: 3}
//    GradoopIds forumToDresdenEdgeIds = GradoopIds.fromLongs(16L, 19L, 20L);
//    // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
//    GradoopIds forumToTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15,17,18] Forum -[__EDGE__]-> Leipzig {count: 3}
//    GradoopIds forumToLeipzigEdgeIds = GradoopIds.fromLongs(15L, 17L, 18L);
//    // [10] Berlin-[__EDGE__]-> Tag {count: 1}
//    GradoopIds berlinToTagEdgeIds = GradoopIds.fromLongs(10L);
//    // [7,9] Dresden-[__EDGE__]-> Tag {count: 2}
//    GradoopIds dresdenToTagEdgeIds = GradoopIds.fromLongs(7L, 9L);
//    // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
//    GradoopIds leipzigToTagEdgeIds = GradoopIds.fromLongs(8L);
//
//    long expectedEdgeCount = 11L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds.contains(e.getId())) {
//        // [3,6,21] Dresden -[__EDGE__]-> Leipzig {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[__EDGE__]-> Leipzig {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDLeipzig, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[__EDGE__]-> Dresden {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDDresden, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
//        // [22,23] Berlin  -[__EDGE__]-> Dresden {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
//          vertexIDDresden, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToDresdenEdgeIds.contains(e.getId())) {
//        // [16,19,20] Forum -[__EDGE__]-> Dresden {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
//          vertexIDDresden, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[__EDGE__]-> Tag {count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum, vertexIDTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToLeipzigEdgeIds.contains(e.getId())) {
//        // [15,17,18] Forum -[__EDGE__]-> Leipzig {count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
//          vertexIDLeipzig, aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToTagEdgeIds.contains(e.getId())) {
//        // [10] Berlin-[__EDGE__]-> Tag {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin, vertexIDTag,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToTagEdgeIds.contains(e.getId())) {
//        // [7,9] Dresden-[__EDGE__]-> Tag {count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden, vertexIDTag,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToTagEdgeIds.contains(e.getId())) {
//        // [8] Leipzig-[__EDGE__]-> Tag {count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig, vertexIDTag,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexLabelAndEdgeProperty() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, edgeGroupingKey, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 1 summarized vertex
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//
//    assertEquals("wrong number of vertices", 1L, vertices.size());
//    GradoopId vertexIDPerson = GradoopId.fromLong(0L);
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//      if (vertexIdsPerson.contains(v.getId())) {
//        vertexIDPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 3 summarized sna_edges:
//    // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
//    GradoopIds personToPersonEdgeIds1 = GradoopIds.fromLongs(0L, 1L, 4L, 5L);
//    // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
//    GradoopIds personToPersonEdgeIds2 = GradoopIds.fromLongs(2L, 3L, 6L);
//    // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
//    GradoopIds personToPersonEdgeIds3 = GradoopIds.fromLongs(21L, 22L, 23L);
//    long expectedEdgeCount = 3L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (personToPersonEdgeIds1.contains(e.getId())) {
//        // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToPersonEdgeIds2.contains(e.getId())) {
//        // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToPersonEdgeIds3.contains(e.getId())) {
//        // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexLabelAndEdgePropertyWithAbsentValue() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, edgeGroupingKey, false)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//    // [6,7,8,] Tag {count: 3}
//    GradoopIds vertexIdsTag = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {count: 2}
//    GradoopIds vertexIdsForum = GradoopIds.fromLongs(9L, 10L);
//
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIDPerson = GradoopId.fromLong(0L);
//    GradoopId vertexIDTag = GradoopId.fromLong(0L);
//    GradoopId vertexIDForum = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPerson.contains(v.getId())) {
//        // [0,1,2,3,4,5] Person {count: 6}
//        vertexIDPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTag.contains(v.getId())) {
//        // [6,7,8,] Tag {count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForum.contains(v.getId())) {
//        // [9,10] Forum {count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 7 summarized sna_edges:
//    // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
//    GradoopIds personToPersonEdgeIds1 = GradoopIds.fromLongs(0L, 1L, 4L, 5L);
//    // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
//    GradoopIds personToPersonEdgeIds2 = GradoopIds.fromLongs(2L, 3L, 6L);
//    // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
//    GradoopIds personToPersonEdgeIds3 = GradoopIds.fromLongs(21L, 22L, 23L);
//    // [7,8,9,10] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
//    GradoopIds personToTagEdgeIds = GradoopIds.fromLongs(7L, 8L, 9L, 10L);
//    // [11,12,13,14] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
//    GradoopIds forumToTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15,17,18,19,20] Forum -[__EDGE__]-> Person {since: __NULL, count: 5}
//    GradoopIds forumToPersonEdgeIds1 =
//      GradoopIds.fromLongs(15L, 17L, 18L, 19L, 20L);
//    // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
//    GradoopIds forumToPersonEdgeIds2 = GradoopIds.fromLongs(16L);
//
//    long expectedEdgeCount = 7L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (personToPersonEdgeIds1.contains(e.getId())) {
//        // [0,1,4,5] Person -[__EDGE__]-> Person {since: 2014, count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToPersonEdgeIds2.contains(e.getId())) {
//        // [2,3,6] Person -[__EDGE__]-> Person {since: 2013, count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToPersonEdgeIds3.contains(e.getId())) {
//        // [21,22,23] Person -[__EDGE__]-> Person {since: 2015, count: 3}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson,
//          vertexIDPerson, edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personToTagEdgeIds.contains(e.getId())) {
//        // [7,8,9,10] Person -[__EDGE__]-> Tag {since: __NULL, count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDPerson, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[__EDGE__]-> Tag {since: __NULL, count: 4}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToPersonEdgeIds1.contains(e.getId())) {
//        // [15,17,18,19,20] Forum -[__EDGE__]-> Person {since: __NULL, count: 5
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
//          vertexIDPerson, edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 5,
//          1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumToPersonEdgeIds2.contains(e.getId())) {
//        // [16] Forum -[__EDGE__]-> Person {since: 2013, count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDForum,
//          vertexIDPerson, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//
//  @Test
//  public void testSummarizeOnVertexLabelAndVertexAndEdgeProperty() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true,
//        edgeGroupingKey, false).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 5 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L), vertexIDDresden = GradoopId.fromLong(0L), vertexIDBerlin = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 6 summarized sna_edges
//    // [4,5] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
//    GradoopIds dresdenToDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
//    GradoopIds dresdenToLeipzigEdgeIds1 = GradoopIds.fromLongs(3L, 6L);
//    // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
//    GradoopIds dresdenToLeipzigEdgeIds2 = GradoopIds.fromLongs(21L);
//    // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
//    GradoopIds leipzigToLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
//    GradoopIds leipzigToDresdenEdgeIds = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [22,23] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
//    GradoopIds berlinToDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//    long expectedEdgeCount = 6L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenToDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[__EDGE__]-> Dresden {since: "2014", count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDDresden, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds1.contains(e.getId())) {
//        // [3,6] Dresden -[__EDGE__]-> Leipzig {since: "2013", count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDLeipzig, edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenToLeipzigEdgeIds2.contains(e.getId())) {
//        // [21] Dresden -[__EDGE__]-> Leipzig {since: "2015", count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDDresden,
//          vertexIDLeipzig, edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[__EDGE__]-> Leipzig {since: "2014", count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDLeipzig, edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigToDresdenEdgeIds.contains(e.getId())) {
//        // [2,3,4] Leipzig -[__EDGE__]-> Dresden {since: "2013", count: 1}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDLeipzig,
//          vertexIDDresden, edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinToDresdenEdgeIds.contains(e.getId())) {
//        // [22,23] Berlin  -[__EDGE__]-> Dresden {since: "2015", count: 2}
//        testEdge(e, GConstants.DEFAULT_EDGE_LABEL, vertexIDBerlin,
//          vertexIDDresden, edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeLabel() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, null, true).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//    // [6,7,8] Tag {count: 3}
//    GradoopIds vertexIdsTag = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {count: 2}
//    GradoopIds vertexIdsForum = GradoopIds.fromLongs(9L, 10L);
//
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIDPerson = GradoopId.fromLong(0L), vertexIDTag = GradoopId.fromLong(0L), vertexIDForum = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPerson.contains(v.getId())) {
//        // [0,1,2,3,4,5] Person {count: 6}
//        vertexIDPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTag.contains(v.getId())) {
//        // [6,7,8] Tag {count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForum.contains(v.getId())) {
//        // [9,10] Forum {count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 5 summarized sna_edges:
//    // [0,1,2,3,4,5,6,21,22,23] Person -[knows]-> Person {count: 10}
//    GradoopIds knowsEdgeIds =
//      GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L, 6L, 21L, 22L, 23L);
//    assertEquals(10, knowsEdgeIds.size());
//    // [7,8,9,10] Person -[hasInterest]-> Tag {count: 4}
//    GradoopIds hasInterestEdgeIds = GradoopIds.fromLongs(7L, 8L, 9L, 10L);
//    assertEquals(4, hasInterestEdgeIds.size());
//    // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
//    GradoopIds hasTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    assertEquals(4, hasTagEdgeIds.size());
//    // [15,16] Forum -[hasModerator]-> Person {count: 2}
//    GradoopIds hasModeratorEdgeIds = GradoopIds.fromLongs(15L, 16L);
//    assertEquals(2, hasModeratorEdgeIds.size());
//    // [17,18,19,20] Forum -[hasMember]-> Person {count: 4}
//    GradoopIds hasMemberEdgeIds = GradoopIds.fromLongs(17L, 18L, 19L, 20L);
//    assertEquals(4, hasMemberEdgeIds.size());
//
//    long expectedEdgeCount = 5L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (knowsEdgeIds.contains(e.getId())) {
//        // [0,1,2,3,4,5,6,21,22,23] Person -[knows]-> Person {count: 10}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          aggregatePropertyKey, 10, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (hasInterestEdgeIds.contains(e.getId())) {
//        // [7,8,9,10] Person -[hasInterest]-> Tag {count: 4}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (hasTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
//        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (hasModeratorEdgeIds.contains(e.getId())) {
//        // [15,16] Forum -[hasModerator]-> Person {count: 2}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (hasMemberEdgeIds.contains(e.getId())) {
//        // [17,18,19,20] Forum -[hasMember]-> Person {count: 4}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDPerson,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeLabelAndVertexProperty() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true, null, true)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 5 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L), vertexIDDresden = GradoopId.fromLong(0L), vertexIDBerlin = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 5 summarized sna_edges
//    // [4,5] Dresden -[knows]-> Dresden {count: 2}
//    GradoopIds dresdenKnowsDresdenEdgeEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
//    GradoopIds dresdenKnowsLeipzig = GradoopIds.fromLongs(3L, 6L, 21L);
//    // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
//    GradoopIds leipzigKnowsLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[knows]-> Dresden {count: 1}
//    GradoopIds leipzigKnowsDresden = GradoopIds.fromLongs(2L);
//    // [22,23] Berlin  -[knows]-> Dresden {count: 2}
//    GradoopIds berlinKnowsDresden = GradoopIds.fromLongs(22L, 23L);
//
//    long expectedEdgeCount = 5L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenKnowsDresdenEdgeEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[knows]-> Dresden {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzig.contains(e.getId())) {
//        // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsLeipzig.contains(e.getId())) {
//        // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsDresden.contains(e.getId())) {
//        // [2] Leipzig -[knows]-> Dresden {count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinKnowsDresden.contains(e.getId())) {
//        // [22,23] Berlin -[knows]-> Dresden {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void
//  testSummarizeOnVertexAndEdgeLabelAndVertexPropertyWithAbsentValue() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String vertexGroupingKey = "city";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true, null, true)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 5 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    // [6,7,8] Tag {city: "NULL", count: 3}
//    GradoopIds vertexIdsTagNull = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {city: "NULL", count: 2}
//    GradoopIds vertexIdsForumNull = GradoopIds.fromLongs(9L, 10L);
//    assertEquals("wrong number of vertices", 5L, vertices.size());
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L), vertexIDDresden = GradoopId.fromLong(0L), vertexIDBerlin = GradoopId.fromLong(0L),
//      vertexIDTag = GradoopId.fromLong(0L), vertexIDForum = GradoopId.fromLong(0L);
//
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTagNull.contains(v.getId())) {
//        // [6,7,8] Tag {city: "NULL", count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForumNull.contains(v.getId())) {
//        // [9,10] Forum {city: "NULL", count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 13 summarized sna_edges:
//    // [4,5] Dresden -[knows]-> Dresden {count: 2}
//    GradoopIds dresdenKnowsDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
//    GradoopIds dresdenKnowsLeipzigEdgeIds = GradoopIds.fromLongs(3L, 6L, 21L);
//    // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
//    GradoopIds leipzigKnowsLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[knows]-> Dresden {count: 1}
//    GradoopIds leipzigKnowsDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22,23] Berlin -[knows]-> Dresden {count: 2}
//    GradoopIds berlinKnowsDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//    // [16] Forum -[hasModerator]-> Dresden {count: 1}
//    GradoopIds forumHasModeratorDresdenEdgeIds = GradoopIds.fromLongs(16L);
//    // [19,20] Forum -[hasMember]-> Dresden {count: 2}
//    GradoopIds forumHasMemberDresdenEdgeIds = GradoopIds.fromLongs(19L, 20L);
//    // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
//    GradoopIds forumHasTagTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15] Forum -[hasModerator]-> Leipzig {count: 1}
//    GradoopIds forumHasModeratorLeipzigEdgeIds = GradoopIds.fromLongs(15L);
//    // [17,18] Forum -[hasMember]-> Leipzig {count: 2}
//    GradoopIds forumHasMemberLeipzigEdgeIds = GradoopIds.fromLongs(17L, 18L);
//    // [10] Berlin -[hasInterest]-> Tag {count: 1}
//    GradoopIds berlinHasInterestTagEdgeIds = GradoopIds.fromLongs(10L);
//    // [7,9] Dresden -[hasInterest]-> Tag {count: 2}
//    GradoopIds dresdenHasInterestTagEdgeIds = GradoopIds.fromLongs(7L, 9L);
//    // [8] Leipzig -[hasInterest]-> Tag {count: 1}
//    GradoopIds leipzigHasInterestTagEdgeIds = GradoopIds.fromLongs(8L);
//    long expectedEdgeCount = 13L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[knows]-> Dresden {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzigEdgeIds.contains(e.getId())) {
//        // [3,6,21] Dresden -[knows]-> Leipzig {count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          aggregatePropertyKey, 3, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[knows]-> Leipzig {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[knows]-> Dresden {count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [22,23] Berlin -[knows]-> Dresden {count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorDresdenEdgeIds.contains(e.getId())) {
//        // [16] Forum -[hasModerator]-> Dresden {count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasMemberDresdenEdgeIds.contains(e.getId())) {
//        // [19,20] Forum -[hasMember]-> Dresden {count: 2}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[hasTag]-> Tag {count: 4}
//        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag,
//          aggregatePropertyKey, 4, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorLeipzigEdgeIds.contains(e.getId())) {
//        // [15] Forum -[hasModerator]-> Leipzig {count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasMemberLeipzigEdgeIds.contains(e.getId())) {
//        // [17,18] Forum -[hasMember]-> Leipzig {count: 2}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinHasInterestTagEdgeIds.contains(e.getId())) {
//        // [10] Berlin -[hasInterest]-> Tag {count: 1}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenHasInterestTagEdgeIds.contains(e.getId())) {
//        // [7,9] Dresden -[hasInterest]-> Tag {count: 2}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
//          aggregatePropertyKey, 2, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigHasInterestTagEdgeIds.contains(e.getId())) {
//        // [8] Leipzig -[hasInterest]-> Tag {count: 1}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDLeipzig, vertexIDTag,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeLabelAndEdgeProperty() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, edgeGroupingKey, true)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 1 summarized vertex
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//
//    assertEquals("wrong number of vertices", 1L, vertices.size());
//    GradoopId vertexIDPerson = GradoopId.fromLong(0L);
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//      if (vertexIdsPerson.contains(v.getId())) {
//        // [0,1,2,3,4,5] Person {count: 6}
//        vertexIDPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 3 summarized sna_edges:
//    // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
//    GradoopIds personKnowsPersonEdgeIds1 = GradoopIds.fromLongs(0L, 1L, 4L, 5L);
//    // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
//    GradoopIds personKnowsPersonEdgeIds2 = GradoopIds.fromLongs(2L, 3L, 6L);
//    // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
//    GradoopIds personKnowsPersonEdgeIds3 = GradoopIds.fromLongs(21L, 22L, 23L);
//    long expectedEdgeCount = 3L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (personKnowsPersonEdgeIds1.contains(e.getId())) {
//        // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personKnowsPersonEdgeIds2.contains(e.getId())) {
//        // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personKnowsPersonEdgeIds3.contains(e.getId())) {
//        // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeLabelAndEdgePropertyWithAbsentValue
//    () throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, null, true, edgeGroupingKey, true)
//        .run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 3 summarized vertices:
//    // [0,1,2,3,4,5] Person {count: 6}
//    GradoopIds vertexIdsPerson = GradoopIds.fromLongs(0L, 1L, 2L, 3L, 4L, 5L);
//    // [6,7,8] Tag {count: 3}
//    GradoopIds vertexIdsTag = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {count: 2}
//    GradoopIds vertexIdsForum = GradoopIds.fromLongs(9L, 10L);
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//
//    GradoopId vertexIDPerson = GradoopId.fromLong(0L), vertexIDTag = GradoopId.fromLong(0L), vertexIDForum = GradoopId.fromLong(0L);
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPerson.contains(v.getId())) {
//        // [0,1,2,3,4,5] Person {count: 6}
//        vertexIDPerson = v.getId();
//        testVertex(v, LABEL_PERSON, aggregatePropertyKey, 6L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTag.contains(v.getId())) {
//        // [6,7,8] Tag {count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, aggregatePropertyKey, 3L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForum.contains(v.getId())) {
//        // [9,10] Forum {count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, aggregatePropertyKey, 2L, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 8 summarized sna_edges:
//    // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
//    GradoopIds personKnowsPersonEdgeIds1 = GradoopIds.fromLongs(0L, 1L, 4L, 5L);
//    // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
//    GradoopIds personKnowsPersonEdgeIds2 = GradoopIds.fromLongs(2L, 3L, 6L);
//    // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
//    GradoopIds personKnowsPersonEdgeIds3 = GradoopIds.fromLongs(21L, 22L, 23L);
//    // [7,8,9,10] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
//    GradoopIds personHasInterestTagEdgeIds =
//      GradoopIds.fromLongs(7L, 8L, 9L, 10L);
//    // [11,12,13,14] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
//    GradoopIds forumHasTagTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
//    GradoopIds forumHasModeratorPersonEdgeIds1 = GradoopIds.fromLongs(15L);
//    // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
//    GradoopIds forumHasModeratorPersonEdgeIds2 = GradoopIds.fromLongs(16L);
//    // [17,18,19,20] Forum -[hasMember]-> Person {since: __NULL, count: 4}
//    GradoopIds forumHasMemberPersonEdgeIds =
//      GradoopIds.fromLongs(17L, 18L, 19L, 20L);
//    long expectedEdgeCount = 8L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (personKnowsPersonEdgeIds1.contains(e.getId())) {
//        // [0,1,4,5] Person -[knows]-> Person {since: 2014, count: 4}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personKnowsPersonEdgeIds2.contains(e.getId())) {
//        // [2,3,6] Person -[knows]-> Person {since: 2013, count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personKnowsPersonEdgeIds3.contains(e.getId())) {
//        // [21,22,23] Person -[knows]-> Person {since: 2015, count: 3}
//        testEdge(e, LABEL_KNOWS, vertexIDPerson, vertexIDPerson,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 3, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (personHasInterestTagEdgeIds.contains(e.getId())) {
//        // [7,8,9,10] Person -[hasInterest]-> Tag {since: __NULL, count: 4}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDPerson, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[hasTag]-> Tag {since: __NULL, count: 4}
//        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
//          NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorPersonEdgeIds1.contains(e.getId())) {
//        // [15] Forum -[hasModerator]-> Person {since: __NULL, count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorPersonEdgeIds2.contains(e.getId())) {
//        // [16] Forum -[hasModerator]-> Person {since: 2013, count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDPerson,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasMemberPersonEdgeIds.contains(e.getId())) {
//        // [17,18,19,20] Forum -[hasMember]-> Person {since: __NULL, count: 4}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDPerson,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgeProperty() throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph =
//      getGraphStore().getGraph(GradoopId.fromLong(0L)).combine(getGraphStore().getGraph(GradoopId.fromLong(1L)))
//        .combine(getGraphStore().getGraph(GradoopId.fromLong(2L)));
//
//    final String vertexGroupingKey = "city";
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true,
//        edgeGroupingKey, true).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//
//    // 3 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    assertEquals("wrong number of vertices", 3L, vertices.size());
//
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L), vertexIDDresden = GradoopId.fromLong(0L), vertexIDBerlin = GradoopId.fromLong(0L);
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 6 summarized sna_edges
//    // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
//    GradoopIds dresdenKnowsDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
//    GradoopIds dresdenKnowsLeipzigEdgeIds1 = GradoopIds.fromLongs(3L, 6L);
//    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
//    GradoopIds dresdenKnowsLeipzigEdgeIds2 = GradoopIds.fromLongs(21L);
//    // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
//    GradoopIds leipzigKnowsLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
//    GradoopIds leipzigKnowsDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
//    GradoopIds berlinKnowsDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//
//    long expectedEdgeCount = 6L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzigEdgeIds1.contains(e.getId())) {
//        // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzigEdgeIds2.contains(e.getId())) {
//        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  @Test
//  public void
//  testSummarizeOnVertexAndEdgeLabelAndVertexAndEdgePropertyWithAbsentValue()
//    throws
//    Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//
//    final String vertexGroupingKey = "city";
//    final String edgeGroupingKey = "since";
//    final String aggregatePropertyKey = "count";
//
//    SummarizationRunner summarizationRunner =
//      new SummarizationRunner(inputGraph, vertexGroupingKey, true,
//        edgeGroupingKey, true).run();
//    List<VertexPojo> vertices = summarizationRunner.getVertices();
//    List<EdgePojo> edges = summarizationRunner.getEdges();
//
//    // 5 summarized vertices:
//    // [0,1] Person {city: "Leipzig", count: 2}
//    GradoopIds vertexIdsPersonLeipzig = GradoopIds.fromLongs(0L, 1L);
//    // [2,3,4] Person {city: "Dresden", count: 3}
//    GradoopIds vertexIdsPersonDresden = GradoopIds.fromLongs(2L, 3L, 4L);
//    // [5] Person {city: "Berlin", count: 1}
//    GradoopIds vertexIdsPersonBerlin = GradoopIds.fromLongs(5L);
//    // [6,7,8] Tag {city: "NULL", count: 3}
//    GradoopIds vertexIdsTagNull = GradoopIds.fromLongs(6L, 7L, 8L);
//    // [9,10] Forum {city: "NULL", count: 2}
//    GradoopIds vertexIdsForumNull = GradoopIds.fromLongs(9L, 10L);
//    assertEquals("wrong number of vertices", 5L, vertices.size());
//
//    GradoopId vertexIDLeipzig = GradoopId.fromLong(0L), vertexIDDresden = GradoopId.fromLong(0L), vertexIDBerlin = GradoopId.fromLong(0L),
//      vertexIDTag = GradoopId.fromLong(0L), vertexIDForum = GradoopId.fromLong(0L);
//    for (EPGMVertex v : vertices) {
//      // check vertex id
//      assertNotNull("vertex id must not be null", v.getId());
//
//      if (vertexIdsPersonLeipzig.contains(v.getId())) {
//        // [0,1] Person {city: "Leipzig", count: 2}
//        vertexIDLeipzig = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Leipzig",
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonDresden.contains(v.getId())) {
//        // [2,3,4] Person {city: "Dresden", count: 3}
//        vertexIDDresden = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Dresden",
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsPersonBerlin.contains(v.getId())) {
//        // [5] Person {city: "Berlin", count: 1}
//        vertexIDBerlin = v.getId();
//        testVertex(v, LABEL_PERSON, vertexGroupingKey, "Berlin",
//          aggregatePropertyKey, 1L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsTagNull.contains(v.getId())) {
//        // [6,7,8] Tag {city: "NULL", count: 3}
//        vertexIDTag = v.getId();
//        testVertex(v, LABEL_TAG, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 3L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (vertexIdsForumNull.contains(v.getId())) {
//        // [9,10] Forum {city: "NULL", count: 2}
//        vertexIDForum = v.getId();
//        testVertex(v, LABEL_FORUM, vertexGroupingKey, NULL_VALUE,
//          aggregatePropertyKey, 2L, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected vertex", false);
//      }
//    }
//
//    // 14 summarized sna_edges:
//    // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
//    GradoopIds dresdenKnowsDresdenEdgeIds = GradoopIds.fromLongs(4L, 5L);
//    // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
//    GradoopIds dresdenKnowsLeipzigEdgeIds1 = GradoopIds.fromLongs(3L, 6L);
//    // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
//    GradoopIds dresdenKnowsLeipzigEdgeIds2 = GradoopIds.fromLongs(21L);
//    // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
//    GradoopIds leipzigKnowsLeipzigEdgeIds = GradoopIds.fromLongs(0L, 1L);
//    // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
//    GradoopIds leipzigKnowsDresdenEdgeIds = GradoopIds.fromLongs(2L);
//    // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
//    GradoopIds berlinKnowsDresdenEdgeIds = GradoopIds.fromLongs(22L, 23L);
//    // [16] Forum -[hasModerator]-> Dresden {since: 2013, count: 1}
//    GradoopIds forumHasModeratorDresdenEdgeIds = GradoopIds.fromLongs(16L);
//    // [19,20] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
//    GradoopIds forumHasMemberDresdenEdgeIds = GradoopIds.fromLongs(19L, 20L);
//    // [11,12,13,14] Forum -[hasTag]-> Tag {since: NULL, count: 4}
//    GradoopIds forumHasTagTagEdgeIds = GradoopIds.fromLongs(11L, 12L, 13L, 14L);
//    // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
//    GradoopIds forumHasModeratorLeipzigEdgeIds = GradoopIds.fromLongs(15L);
//    // [17,18] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
//    GradoopIds forumHasMemberLeipzigEdgeIds = GradoopIds.fromLongs(17L, 18L);
//    // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
//    GradoopIds berlinHasInterestTagEdgeIds = GradoopIds.fromLongs(10L);
//    // [7,9] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
//    GradoopIds dresdenHasInterestTagEdgeIds = GradoopIds.fromLongs(7L, 9L);
//    // [8] Leipzig -[hasInterest]-> Tag {since: NULL, count: 1}
//    GradoopIds leipzigHasInterestTagEdgeIds = GradoopIds.fromLongs(8L);
//
//    long expectedEdgeCount = 14L;
//    assertEquals("wrong number of edges", expectedEdgeCount, edges.size());
//
//    for (EPGMEdge e : edges) {
//      // check edge id
//      assertNotNull("edge id must not be null", e.getId());
//
//      if (dresdenKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [4,5] Dresden -[knows]-> Dresden {since: 2014, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDDresden,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzigEdgeIds1.contains(e.getId())) {
//        // [3,6] Dresden -[knows]-> Leipzig {since: 2013, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenKnowsLeipzigEdgeIds2.contains(e.getId())) {
//        // [21] Dresden -[knows]-> Leipzig {since: 2015, count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDDresden, vertexIDLeipzig,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsLeipzigEdgeIds.contains(e.getId())) {
//        // [0,1] Leipzig -[knows]-> Leipzig {since: 2014, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDLeipzig,
//          edgeGroupingKey, "2014", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [2] Leipzig -[knows]-> Dresden {since: 2013, count: 1}
//        testEdge(e, LABEL_KNOWS, vertexIDLeipzig, vertexIDDresden,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinKnowsDresdenEdgeIds.contains(e.getId())) {
//        // [22,23] Berlin -[knows]-> Dresden {since: 2015, count: 2}
//        testEdge(e, LABEL_KNOWS, vertexIDBerlin, vertexIDDresden,
//          edgeGroupingKey, "2015", aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorDresdenEdgeIds.contains(e.getId())) {
//        // [16] Forum -[hasModerator]-> Dresden {since: 2013, count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDDresden,
//          edgeGroupingKey, "2013", aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasMemberDresdenEdgeIds.contains(e.getId())) {
//        // [19,20] Forum -[hasMember]-> Dresden {since: NULL, count: 2}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDDresden,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasTagTagEdgeIds.contains(e.getId())) {
//        // [11,12,13,14] Forum -[hasTag]-> Tag {since: NULL, count: 4}
//        testEdge(e, LABEL_HAS_TAG, vertexIDForum, vertexIDTag, edgeGroupingKey,
//          NULL_VALUE, aggregatePropertyKey, 4, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasModeratorLeipzigEdgeIds.contains(e.getId())) {
//        // [15] Forum -[hasModerator]-> Leipzig {since: NULL, count: 1}
//        testEdge(e, LABEL_HAS_MODERATOR, vertexIDForum, vertexIDLeipzig,
//          aggregatePropertyKey, 1, 1, FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (forumHasMemberLeipzigEdgeIds.contains(e.getId())) {
//        // [17,18] Forum -[hasMember]-> Leipzig {since: NULL, count: 2}
//        testEdge(e, LABEL_HAS_MEMBER, vertexIDForum, vertexIDLeipzig,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (berlinHasInterestTagEdgeIds.contains(e.getId())) {
//        // [10] Berlin -[hasInterest]-> Tag {since: NULL, count: 1}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDBerlin, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (dresdenHasInterestTagEdgeIds.contains(e.getId())) {
//        // [7,9] Dresden -[hasInterest]-> Tag {since: NULL, count: 2}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDDresden, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 2, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else if (leipzigHasInterestTagEdgeIds.contains(e.getId())) {
//        // [8] Leipzig -[hasInterest]-> Tag {since: NULL, count: 1}
//        testEdge(e, LABEL_HAS_INTEREST, vertexIDLeipzig, vertexIDTag,
//          edgeGroupingKey, NULL_VALUE, aggregatePropertyKey, 1, 1,
//          FlinkConstants.SUMMARIZE_GRAPH_ID);
//      } else {
//        assertTrue("unexpected edge: " + e.getId(), false);
//      }
//    }
//  }
//
//  private void testVertex(EPGMVertex vertex, String expectedVertexLabel,
//    String aggregatePropertyKey, Long expectedCountValue,
//    int expectedGraphCount, GradoopId expectedGraphID) {
//    testVertex(vertex, expectedVertexLabel, null, null, aggregatePropertyKey,
//      expectedCountValue, expectedGraphCount, expectedGraphID);
//  }
//
//  private void testVertex(EPGMVertex vertex, String expectedVertexLabel,
//    String vertexGroupingKey, String expectedVertexGroupingValue,
//    String aggregatePropertyKey, Long expectedCountValue,
//    int expectedGraphCount, GradoopId expectedGraphID) {
//    assertEquals("wrong vertex label", expectedVertexLabel, vertex.getLabel());
//    if (vertexGroupingKey != null && expectedVertexGroupingValue != null) {
//      assertEquals("wrong property value", expectedVertexGroupingValue,
//        vertex.getProperty(vertexGroupingKey));
//    }
//    assertEquals("wrong vertex property", expectedCountValue,
//      vertex.getProperty(aggregatePropertyKey));
//    assertEquals("wrong number of graphs", expectedGraphCount,
//      vertex.getGraphCount());
//    assertTrue("wrong graph id", vertex.getGraphIds().contains(expectedGraphID));
//  }
//
//  private void testEdge(EPGMEdge edge, String expectedEdgeLabel,
//    GradoopId expectedSourceVertex, GradoopId expectedTargetVertex,
//    String aggregatePropertyKey, Integer expectedCountValue,
//    int expectedGraphCount, GradoopId expectedGraphID) {
//    testEdge(edge, expectedEdgeLabel, expectedSourceVertex,
//      expectedTargetVertex, null, null, aggregatePropertyKey,
//      expectedCountValue, expectedGraphCount, expectedGraphID);
//  }
//
//  private void testEdge(EPGMEdge edge, String expectedEdgeLabel,
//    GradoopId expectedSourceVertex, GradoopId expectedTargetVertex,
//    String edgeGroupingKey, String expectedGroupingValue,
//    String aggregatePropertyKey, Integer expectedCountValue,
//    int expectedGraphCount, GradoopId expectedGraphID) {
//    assertEquals("wrong edge label", expectedEdgeLabel, edge.getLabel());
//    assertEquals("wrong source vertex", expectedSourceVertex,
//      edge.getSourceVertexId());
//    assertEquals("wrong target vertex", expectedTargetVertex,
//      edge.getTargetVertexId());
//    assertEquals("wrong edge property", expectedCountValue,
//      edge.getProperty(aggregatePropertyKey));
//    assertEquals("wrong number of graphs", expectedGraphCount,
//      edge.getGraphIds().size());
//    assertTrue("wrong graph id", edge.getGraphIds().contains(expectedGraphID));
//
//    if (edgeGroupingKey != null && expectedGroupingValue != null) {
//      assertEquals("wrong group value", expectedGroupingValue,
//        edge.getProperty(edgeGroupingKey));
//    }
//  }

  private class SummarizationRunner {
    private LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> inputGraph;
    private String vertexGroupingKey;
    private final boolean useVertexLabels;
    private final String edgeGroupingKey;
    private final boolean useEdgeLabels;
    private List<VertexPojo> vertices;
    private List<EdgePojo> edges;

    public SummarizationRunner(
      LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
        inputGraph,
      String vertexGroupingKey, boolean useVertexLabels, String edgeGroupingKey,
      boolean useEdgeLabels) {
      this.inputGraph = inputGraph;
      this.vertexGroupingKey = vertexGroupingKey;
      this.useVertexLabels = useVertexLabels;
      this.edgeGroupingKey = edgeGroupingKey;
      this.useEdgeLabels = useEdgeLabels;
    }

    public List<VertexPojo> getVertices() {
      return vertices;
    }

    public List<EdgePojo> getEdges() {
      return edges;
    }

    public LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> run()
      throws Exception {
      Summarization<VertexPojo, EdgePojo, GraphHeadPojo>
        summarization = getSummarizationImpl(vertexGroupingKey, useVertexLabels,
        edgeGroupingKey, useEdgeLabels);

      return summarization.execute(inputGraph);
    }
  }
}
