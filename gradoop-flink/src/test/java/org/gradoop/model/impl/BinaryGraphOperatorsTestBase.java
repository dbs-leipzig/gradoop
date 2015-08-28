package org.gradoop.model.impl;

import org.gradoop.model.EdgeData;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.VertexData;

import java.util.Collection;

import static org.junit.Assert.*;

public class BinaryGraphOperatorsTestBase extends FlinkTestBase {

  public BinaryGraphOperatorsTestBase(TestExecutionMode mode) {
    super(mode);
  }

  protected void performTest(
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      resultGraph,
    long expectedVertexCount, long expectedEdgeCount) throws Exception {
    assertNotNull("resulting graph was null", resultGraph);

    long newGraphID = resultGraph.getId();

    assertEquals("wrong number of vertices", expectedVertexCount,
      resultGraph.getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      resultGraph.getEdgeCount());

    Collection<DefaultVertexData> vertexData =
      resultGraph.getVertices().collect();
    Collection<DefaultEdgeData> edgeData = resultGraph.getEdges().collect();

    checkVertexAndEdgeCount(expectedVertexCount, expectedEdgeCount, vertexData,
      edgeData);

    checkGraphContainment(newGraphID, vertexData, edgeData);
  }

  protected void checkGraphContainment(long newGraphID,
    Collection<DefaultVertexData> vertexData,
    Collection<DefaultEdgeData> edgeData) {
    for (VertexData v : vertexData) {
      assertTrue("vertex is not in new graph",
        v.getGraphs().contains(newGraphID));
    }

    for (EdgeData e : edgeData) {
      assertTrue("edge is not in new graph",
        e.getGraphs().contains(newGraphID));
    }
  }

  protected void checkVertexAndEdgeCount(long expectedVertexCount,
    long expectedEdgeCount, Collection<DefaultVertexData> vertexData,
    Collection<DefaultEdgeData> edgeData) {
    assertEquals("wrong number of vertex values", expectedVertexCount,
      vertexData.size());
    assertEquals("wrong number of edge values", expectedEdgeCount,
      edgeData.size());
  }
}
