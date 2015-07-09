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
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.*;

public class EPGraphSummarizeTests extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphSummarizeTests() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void testSummarizeWithVertexGroupingKey() throws Exception {

    EPGraph communityGraph = graphStore.getGraph(2L);

    final String aggregatePropertyKey = "count";

    final String vertexGroupingKey = "city";
    final String edgeGroupingKey = "";

    // aggregate the number of summarized vertices
    Aggregate<Iterable<EPVertexData>, Long> vertexAggregateFunc =
      new Aggregate<Iterable<EPVertexData>, Long>() {
        @Override
        public Long aggregate(Iterable<EPVertexData> summarizedVertices) throws
          Exception {
          long sum = 0L;
          for (EPVertexData ignored : summarizedVertices) {
            sum++;
          }
          return sum;
        }
      };

    // aggregate the number of summarized edges
    Aggregate<Iterable<EPEdgeData>, Long> edgeAggregateFunc =
      new Aggregate<Iterable<EPEdgeData>, Long>() {
        @Override
        public Long aggregate(Iterable<EPEdgeData> summarizedEdges) throws
          Exception {
          long sum = 0L;
          for (EPEdgeData ignored : summarizedEdges) {
            sum++;
          }
          return sum;
        }
      };

    EPGraph summarizedGraph = communityGraph
      .summarize(vertexGroupingKey, vertexAggregateFunc, edgeGroupingKey,
        edgeAggregateFunc);

    assertNotNull("summarized graph must not be null", summarizedGraph);
    // two vertices: dresden and leipzig
    assertEquals("wrong number of vertices", 2L,
      summarizedGraph.getVertexCount());

    long vertexIDDresden = 0L, vertexIDLeipzig = 0L;

    for (EPVertexData v : summarizedGraph.getVertices().collect()) {
      // check vertex id
      assertNotNull("vertex is must not be null", v.getId());
      // check label
      assertEquals("summarized vertex has wrong label", LABEL_PERSON,
        v.getLabel());
      // check property count
      assertEquals("summarized vertex has wrong property count", 2,
        v.getPropertyCount());
      // both vertices summarize 2 vertices
      assertEquals("vertex has wrong count value", 2,
        v.getProperty(aggregatePropertyKey));
      // vertex needs to represent Dresden or Leipzig
      assertTrue("wrong property value",
        v.getProperty(vertexGroupingKey).equals("Dresden") ||
          v.getProperty(vertexGroupingKey).equals("Leipzig"));
      // both vertices are in one new graph
      assertEquals("wrong number of graphs", 1, v.getGraphs().size());
      assertTrue("wrong graph id",
        v.getGraphs().contains(FlinkConstants.SUMMARIZE_GRAPH_ID));

      if (v.getProperty(vertexGroupingKey).equals("Dresden")) {
        vertexIDDresden = v.getId();
      } else {
        vertexIDLeipzig = v.getId();
      }
    }

    // four edges:
    // Person:Leipzig -[knows]-> Person:Leipzig {count: 2}
    // Person:Leipzig -[knows]-> Person:Dresden {count: 1}
    // Person:Dresden -[knows]-> Person:Leipzig {count: 1}
    // Person:Dresden -[knows]-> Person:Dresden {count: 2}
    assertEquals("wrong number of edges", 4L, summarizedGraph.getEdgeCount());

    for (EPEdgeData e : summarizedGraph.getEdges().collect()) {
      // check edge id
      assertNotNull("edge is must not be null", e.getId());
      // all edges have the "knows label"
      assertEquals("edge has wrong label", LABEL_KNOWS, e.getLabel());
      if (e.getSourceVertex().equals(vertexIDDresden)) {
        // Person:Dresden -[knows]-> Person:Dresden {count: 2}
        if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("wrong edge property", 2,
            e.getProperty(aggregatePropertyKey));
          // Person:Dresden -[knows]-> Person:Leipzig {count: 1}
        } else if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("wrong edge property", 1,
            e.getProperty(aggregatePropertyKey));
        } else {
          assertTrue("unexpected outgoing edge from Dresden", false);
        }
      } else if (e.getSourceVertex().equals(vertexIDLeipzig)) {
        // Person:Leipzig -[knows]-> Person:Leipzig {count: 2}
        if (e.getTargetVertex().equals(vertexIDLeipzig)) {
          assertEquals("wrong edge property", 2,
            e.getProperty(aggregatePropertyKey));
          // Person:Leipzig -[knows]-> Person:Dresden {count: 1}
        } else if (e.getTargetVertex().equals(vertexIDDresden)) {
          assertEquals("wrong edge property", 1,
            e.getProperty(aggregatePropertyKey));
        } else {
          assertTrue("unexpected outgoing edge from Leipzig", false);
        }
      }
    }
  }
}
