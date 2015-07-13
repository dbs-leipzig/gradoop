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
import org.gradoop.model.helper.MathHelper;
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
      // check label
      assertEquals("summarized vertex has wrong label",
        FlinkConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
      // check property count
      assertEquals("summarized vertex has wrong property count", 2,
        v.getPropertyCount());

      if (v.getId().equals(vertexIDLeipzig)) {
        assertEquals("wrong property value", "Leipzig",
          v.getProperty(vertexGroupingKey));
        assertEquals("leipzig has wrong count value", 2,
          v.getProperty(aggregatePropertyKey));
      } else if (v.getId().equals(vertexIDDresden)) {
        assertEquals("wrong property value", "Dresden",
          v.getProperty(vertexGroupingKey));
        assertEquals("dresden has wrong count value", 2,
          v.getProperty(aggregatePropertyKey));
      }

      // both vertices are in one new graph
      assertEquals("wrong number of graphs", 1, v.getGraphs().size());
      assertTrue("wrong graph id",
        v.getGraphs().contains(FlinkConstants.SUMMARIZE_GRAPH_ID));
    }

    // four summarized edges:
    // Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // Leipzig -[__EDGE__]-> Dresden {count: 1}
    // Dresden -[__EDGE__]-> Leipzig {count: 1}
    // Dresden -[__EDGE__]-> Dresden {count: 2}
    assertEquals("wrong number of edges", 4L, summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());
      // all edges have the "knows label"
      assertEquals("edge has wrong label", FlinkConstants.DEFAULT_EDGE_LABEL,
        e.getLabel());
      if (e.getSourceVertex().equals(vertexIDDresden)) {
        // Dresden -[__EDGE__]-> Dresden {count: 2}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("wrong edge property", 2,
            e.getProperty(aggregatePropertyKey));
          // Dresden -[__EDGE__]-> Leipzig {count: 1}
        } else if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("wrong edge property", 1,
            e.getProperty(aggregatePropertyKey));
        } else {
          assertTrue("unexpected outgoing edge from Dresden", false);
        }
      } else if (e.getSourceVertex().equals(vertexIDLeipzig)) {
        // Leipzig -[__EDGE__]-> Leipzig {count: 2}
        if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("wrong edge property", 2,
            e.getProperty(aggregatePropertyKey));
          // Leipzig -[__EDGE__]-> Dresden {count: 1}
        } else if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("wrong edge property", 1,
            e.getProperty(aggregatePropertyKey));
        } else {
          assertTrue("unexpected outgoing edge from Leipzig", false);
        }
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
      // check label
      assertEquals("summarized vertex has wrong label",
        FlinkConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
      // check property count
      assertEquals("summarized vertex has wrong property count", 2,
        v.getPropertyCount());

      Object groupPropertyValue = v.getProperty(vertexGroupingKey);
      Object groupAggregateValue = v.getProperty(aggregatePropertyKey);

      if (v.getId().equals(vertexIDLeipzig)) {
        assertEquals("wrong property value", "Leipzig", groupPropertyValue);
        assertEquals("Leipzig has wrong count value", 2, groupAggregateValue);
      } else if (v.getId().equals(vertexIDDresden)) {
        assertEquals("wrong property value", "Dresden", groupPropertyValue);
        assertEquals("Dresden has wrong count value", 3, groupAggregateValue);
      } else if (v.getId().equals(vertexIDBerlin)) {
        assertEquals("wrong property value", "Berlin", groupPropertyValue);
        assertEquals("Berlin has wrong count value", 1, groupAggregateValue);
      }

      // both vertices are in one new graph
      assertEquals("wrong number of graphs", 1, v.getGraphs().size());
      assertTrue("wrong graph id",
        v.getGraphs().contains(FlinkConstants.SUMMARIZE_GRAPH_ID));
    }

    // four summarized edges:
    // Dresden -[__EDGE__]-> Dresden {count: 2}
    // Dresden -[__EDGE__]-> Leipzig {count: 3}
    // Leipzig -[__EDGE__]-> Leipzig {count: 2}
    // Leipzig -[__EDGE__]-> Dresden {count: 1}
    // Berlin  -[__EDGE__]-> Dresden {count: 2}
    assertEquals("wrong number of edges", 5L, summarizedGraph.getEdgeCount());

    int checkedEdges = 0;
    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge id must not be null", e.getId());
      // all edges have the "knows label"
      assertEquals("edge has wrong label", FlinkConstants.DEFAULT_EDGE_LABEL,
        e.getLabel());
      Object edgeAggregateValue = e.getProperty(aggregatePropertyKey);
      // Dresden
      if (e.getSourceVertex().equals(vertexIDDresden)) {
        // Dresden -[__EDGE__]-> Dresden {count: 2}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("edge id was wrong",
            new Long(MathHelper.cantor(vertexIDDresden, vertexIDDresden)),
            e.getId());

          assertEquals("wrong edge property", 2, edgeAggregateValue);
          checkedEdges++;
          // Dresden -[__EDGE__]-> Leipzig {count: 3}
        } else if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("edge id was wrong",
            new Long(MathHelper.cantor(vertexIDDresden, vertexIDLeipzig)),
            e.getId());
          assertEquals("wrong edge property", 3, edgeAggregateValue);
          checkedEdges++;
        } else {
          assertTrue("unexpected outgoing edge from Dresden", false);
        }
        // Leipzig
      } else if (e.getSourceVertex().equals(vertexIDLeipzig)) {
        // Leipzig -[__EDGE__]-> Leipzig {count: 2}
        if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("edge id was wrong",
            new Long(MathHelper.cantor(vertexIDLeipzig, vertexIDLeipzig)),
            e.getId());
          assertEquals("wrong edge property", 2, edgeAggregateValue);
          checkedEdges++;
          // Leipzig -[__EDGE__]-> Dresden {count: 1}
        } else if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("edge id was wrong",
            new Long(MathHelper.cantor(vertexIDLeipzig, vertexIDDresden)),
            e.getId());
          assertEquals("wrong edge property", 1, edgeAggregateValue);
          checkedEdges++;
        } else {
          assertTrue("unexpected outgoing edge from Leipzig", false);
        }
      } else if (e.getSourceVertex().equals(vertexIDBerlin)) {
        // Berlin -[__EDGE__]-> Dresden {count: 2}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("wrong edge property", 2, edgeAggregateValue);
          checkedEdges++;
        } else {
          assertTrue("unexpected outgoing edge from Leipzig", false);
        }
      }
    }
    assertEquals("wrong number of checked edges", 5, checkedEdges);
  }

  @Test
  public void testSummarizeWithAbsentVertexGroupingKey() throws Exception {
    EPGraph input = graphStore.getGraph(3L);

    final String vertexGroupingKey = "city";
    final String aggregatePropertyKey = "count";

    EPGraph summarizedGraph = input.summarize(vertexGroupingKey);

    assertNotNull("summarized graph must not be null", summarizedGraph);

    // two summarized vertices:
    // 2 __VERTEX__ {city: "Dresden", count: 3}
    // 10 __VERTEX__ {city: "__DEFAULT_GROUP", count: 1}
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());
    long vertexIDDresden = 2L, vertexIDGraphProcessingForum = 10L;
    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex id must not be null", v.getId());
      // check label
      assertEquals("summarized vertex has wrong label",
        FlinkConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
      // check property count
      assertEquals("summarized vertex has wrong property count", 2,
        v.getPropertyCount());

      Object groupPropertyValue = v.getProperty(vertexGroupingKey);
      Object groupAggregateValue = v.getProperty(aggregatePropertyKey);

      if (v.getId().equals(vertexIDDresden)) {
        assertEquals("wrong property value", "Dresden", groupPropertyValue);
        assertEquals("Dresden has wrong count value", 2, groupAggregateValue);
      } else if (v.getId().equals(vertexIDGraphProcessingForum)) {
        assertEquals("wrong property value", Summarization.DEFAULT_VERTEX_GROUP,
          groupPropertyValue);
        assertEquals("Default has wrong count value", 1, groupAggregateValue);
      }

      // both vertices are in one new graph
      assertEquals("wrong number of graphs", 1, v.getGraphs().size());
      assertTrue("wrong graph id",
        v.getGraphs().contains(FlinkConstants.SUMMARIZE_GRAPH_ID));
    }

    // two summarized edges:
    // Default -[__EDGE__]-> Dresden {count: 3}
    // Dresden -[__EDGE__]-> Dresden {count: 1}
    assertEquals("wrong number of edges", 2L, summarizedGraph.getEdgeCount());

    int checkedEdges = 0;
    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge is must not be null", e.getId());
      // all edges have the "knows label"
      assertEquals("edge has wrong label", FlinkConstants.DEFAULT_EDGE_LABEL,
        e.getLabel());
      Object edgeAggregateValue = e.getProperty(aggregatePropertyKey);
      // Dresden
      if (e.getSourceVertex().equals(vertexIDGraphProcessingForum)) {
        // Default -[__EDGE__]-> Dresden {count: 3}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("edge id was wrong", new Long(
            MathHelper.cantor(vertexIDGraphProcessingForum, vertexIDDresden)),
            e.getId());
          assertEquals("wrong edge property", 3, edgeAggregateValue);
          checkedEdges++;
        } else {
          assertTrue("unexpected outgoing edge from Dresden", false);
        }
        // Dresden
      } else if (e.getSourceVertex().equals(vertexIDDresden)) {
        // Dresden -[__EDGE__]-> Dresden {count: 1}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("edge id was wrong",
            new Long(MathHelper.cantor(vertexIDDresden, vertexIDDresden)),
            e.getId());
          assertEquals("wrong edge property", 1, edgeAggregateValue);
          checkedEdges++;
        } else {
          assertTrue("unexpected outgoing edge from Leipzig", false);
        }
      }
    }
    assertEquals("wrong number of checked edges", 2, checkedEdges);
  }
}
