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

import org.gradoop.model.EdgeData;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.VertexData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Set;

import static org.gradoop.GradoopTestBaseUtils.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphExcludeTest extends BinaryGraphOperatorsTestBase {

  public LogicalGraphExcludeTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSameGraph() throws Exception {
    // "0, 0, 0, 0"
    Long firstGraph = 0L;
    Long secondGraph = 0L;
    long expectedVertexCount = 0L;
    long expectedEdgeCount = 0L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      graphStore.getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      graphStore.getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.exclude(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    // "0, 2, 1, 0"
    Long firstGraph = 0L;
    Long secondGraph = 2L;
    long expectedVertexCount = 1L;
    long expectedEdgeCount = 0L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      graphStore.getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      graphStore.getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.exclude(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingSwitchedGraphs() throws Exception {
    // "2, 0, 2, 2"
    Long firstGraph = 2L;
    Long secondGraph = 0L;
    long expectedVertexCount = 2L;
    long expectedEdgeCount = 2L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      graphStore.getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      graphStore.getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.exclude(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    // "0, 1, 3, 4"
    Long firstGraph = 0L;
    Long secondGraph = 1L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      graphStore.getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      graphStore.getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.exclude(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingSwitchedGraphs() throws Exception {
    // "1, 0, 3, 4"
    Long firstGraph = 1L;
    Long secondGraph = 0L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      graphStore.getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      graphStore.getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.exclude(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testAssignment() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      databaseCommunity = graphStore.getGraph(0L);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      hadoopCommunity = graphStore.getGraph(1L);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = databaseCommunity.exclude(hadoopCommunity);

    Collection<DefaultVertexData> vertexData = newGraph.getVertices().collect();
    Collection<DefaultEdgeData> edgeData = newGraph.getEdges().collect();

    for (VertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      if (v.equals(VERTEX_PERSON_ALICE)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_BOB)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(VERTEX_PERSON_EVE)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }

    for (EdgeData e : edgeData) {
      Set<Long> gIDs = e.getGraphs();
      if (e.equals(EDGE_0_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(EDGE_1_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(EDGE_6_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      } else if (e.equals(EDGE_21_KNOWS)) {
        assertEquals("wrong number of graphs", 2, gIDs.size());
      }
    }
  }
}
