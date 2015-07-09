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
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class EPGraphTests extends EPFlinkTest {

  @Test
  public void testGetVertices() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertEquals("wrong number of vertices", 11, dbGraph.getVertices().size());
  }

  @Test
  public void testGetEdges() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph dbGraph = graphStore.getDatabaseGraph();

    assertEquals("wrong number of edges", 24, dbGraph.getEdges().size());

    assertEquals("wrong number of outgoing edges", 2,
      dbGraph.getOutgoingEdges(alice.getId()).size());

    assertEquals("wrong number of outgoing edges", 0,
      dbGraph.getOutgoingEdges(tagDatabases.getId()).size());

    assertEquals("wrong number of incoming edges", 3,
      dbGraph.getIncomingEdges(tagDatabases.getId()).size());

    assertEquals("wrong number of incoming edges", 0,
      dbGraph.getIncomingEdges(frank.getId()).size());
  }

  public void testMatch() throws Exception {

  }

  public void testProject() throws Exception {

  }

//  @Test
  public void testSummarizeWithVertexGroupingKeys() throws Exception {
    EPGraphStore graphStore = createSocialGraph();

    EPGraph communityGraph = graphStore.getGraph(2L);

    final String aggregatePropertyKey = "count";
    String vertexGroupingKey = "city";

    List<String> vertexGroupingKeys = Lists.newArrayList(vertexGroupingKey);
    List<String> edgeGroupingKeys = Lists.newArrayListWithCapacity(0);

    // aggregate the number of summarized vertices
    Aggregate<EPVertexCollection, Long> vertexAggregateFunc =
      new Aggregate<EPVertexCollection, Long>() {
        @Override
        public Long aggregate(EPVertexCollection summarizedVertices) throws
          Exception {
          return summarizedVertices.size();
        }
      };

    // aggregate the number of summarized edges
    Aggregate<EPEdgeCollection, Long> edgeAggregateFunc =
      new Aggregate<EPEdgeCollection, Long>() {
        @Override
        public Long aggregate(EPEdgeCollection summarizedEdges) throws
          Exception {
          return summarizedEdges.size();
        }
      };

    EPGraph summarizedGraph = communityGraph
      .summarize(vertexGroupingKeys, vertexAggregateFunc, edgeGroupingKeys,
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

  public void testCallForGraph() throws Exception {

  }

  public void testCallForCollection() throws Exception {

  }
}