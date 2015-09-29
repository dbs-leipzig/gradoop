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

import org.gradoop.GradoopTestBaseUtils;
import org.gradoop.model.EdgeData;
import org.gradoop.model.VertexData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LogicalGraphOverlapTest extends BinaryGraphOperatorsTestBase {

  public LogicalGraphOverlapTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testSameGraph() throws Exception {
    Long firstGraph = 0L;
    Long secondGraph = 0L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingGraphs() throws Exception {
    Long firstGraph = 0L;
    Long secondGraph = 2L;
    long expectedVertexCount = 2L;
    long expectedEdgeCount = 2L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingSwitchedGraphs() throws Exception {
    Long firstGraph = 2L;
    Long secondGraph = 0L;
    long expectedVertexCount = 2L;
    long expectedEdgeCount = 2L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingGraphs() throws Exception {
    Long firstGraph = 0L;
    Long secondGraph = 1L;
    long expectedVertexCount = 0L;
    long expectedEdgeCount = 0L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testNonOverlappingSwitchedGraphs() throws Exception {
    Long firstGraph = 1L;
    Long secondGraph = 0L;
    long expectedVertexCount = 0L;
    long expectedEdgeCount = 0L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingVertexSetGraphs() throws Exception {
    Long firstGraph = 3L;
    Long secondGraph = 1L;
    long expectedVertexCount = 2L;
    long expectedEdgeCount = 1L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testOverlappingVertexSetSwitchedGraphs() throws Exception {
    Long firstGraph = 1L;
    Long secondGraph = 3L;
    long expectedVertexCount = 2L;
    long expectedEdgeCount = 1L;

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> first =
      getGraphStore().getGraph(firstGraph);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> second =
      getGraphStore().getGraph(secondGraph);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> result =
      first.overlap(second);

    performTest(result, expectedVertexCount, expectedEdgeCount);
  }

  @Test
  public void testAssignment() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      databaseCommunity = getGraphStore().getGraph(0L);
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphCommunity = getGraphStore().getGraph(2L);

    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = graphCommunity.overlap(databaseCommunity);

    Collection<DefaultVertexData> vertexData = newGraph.getVertices().collect();
    Collection<DefaultEdgeData> edgeData = newGraph.getEdges().collect();

    for (VertexData v : vertexData) {
      Set<Long> gIDs = v.getGraphs();
      if (v.equals(GradoopTestBaseUtils.VERTEX_PERSON_ALICE)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (v.equals(GradoopTestBaseUtils.VERTEX_PERSON_BOB)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      }
    }

    for (EdgeData e : edgeData) {
      Set<Long> gIDs = e.getGraphs();
      if (e.equals(GradoopTestBaseUtils.EDGE_0_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      } else if (e.equals(GradoopTestBaseUtils.EDGE_1_KNOWS)) {
        assertEquals("wrong number of graphs", 3, gIDs.size());
      }
    }
  }
}
