package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

public class BinaryGraphOperatorsTestBase extends FlinkTestBase {

  public BinaryGraphOperatorsTestBase(TestExecutionMode mode) {
    super(mode);
  }

  protected void performTest(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      resultGraph,
    long expectedVertexCount, long expectedEdgeCount) throws Exception {
    assertNotNull("resulting graph was null", resultGraph);

    GradoopId newGraphID = resultGraph.getId();

    assertEquals("wrong number of vertices", expectedVertexCount,
      resultGraph.getVertexCount());
    assertEquals("wrong number of edges", expectedEdgeCount,
      resultGraph.getEdgeCount());

    Collection<VertexPojo> vertexData = resultGraph.getVertices()
      .collect();
    Collection<EdgePojo> edgeData = resultGraph.getEdges()
      .collect();

    checkVertexAndEdgeCount(expectedVertexCount, expectedEdgeCount, vertexData,
      edgeData);

    checkGraphContainment(newGraphID, vertexData, edgeData);
  }

  protected void checkGraphContainment(GradoopId newGraphID,
    Collection<VertexPojo> vertexData,
    Collection<EdgePojo> edgeData) {
    for (VertexPojo v : vertexData) {
      assertTrue("vertex is not in new graph",
        v.getGraphIds().contains(newGraphID));
    }

    for (EdgePojo e : edgeData) {
      assertTrue("edge is not in new graph", e.getGraphIds().contains
        (newGraphID));
    }
  }

  protected void checkVertexAndEdgeCount(long expectedVertexCount,
    long expectedEdgeCount,
    Collection<VertexPojo> vertexData,
    Collection<EdgePojo> edgeData) {
    assertEquals("wrong number of vertex values", expectedVertexCount,
      vertexData.size());
    assertEquals("wrong number of edge values", expectedEdgeCount,
      edgeData.size());
  }

  protected void checkElementMatches(Set<EPGMGraphElement> inElements,
    Set<EPGMGraphElement> outElements) {
    for(EPGMGraphElement outElement : outElements) {
      boolean match = false;

      String elementClassName = outElement.getClass().getSimpleName();

      for(EPGMGraphElement inVertex : inElements) {
        if (outElement.getId().equals(inVertex.getId())) {
          assertEquals(
            "wrong number of graphs for " + elementClassName,
            inVertex.getGraphCount() + 1,
            outElement.getGraphCount()
          );
          match = true;
          break;
        }
      }
      assertTrue("expected " + elementClassName + " not found",match);
    }
  }
}
