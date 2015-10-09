package org.gradoop.model.impl.algorithms.epgmlabelpropagation;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagation;
import org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the algorithm using the Gradoop Graph.callForCollection() method.
 */
@RunWith(Parameterized.class)
public class EPGMLabelPropagationGradoopTest extends FlinkTestBase {

  final String propertyKey = EPGMLabelPropagationAlgorithm.CURRENT_VALUE;

  public EPGMLabelPropagationGradoopTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testLabelPropagationWithCallByPropertyKey() throws Exception {
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      inputGraph = getGraphStore().getGraph(2L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      labeledGraphCollection = inputGraph.callForCollection(
      new EPGMLabelPropagation<DefaultVertexData, DefaultEdgeData,
              DefaultGraphData>(2, propertyKey, getExecutionEnvironment()));
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> sub1 =
      labeledGraphCollection.getGraph(-2L);
    assertEquals("Sub graph has no edge", 0L, sub1.getEdgeCount());
    assertEquals("sub graph has two vertices", 2L, sub1.getVertexCount());
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> sub2 =
      labeledGraphCollection.getGraph(-1L);
    assertEquals("Sub graph has no edge", 0L, sub2.getEdgeCount());
    assertEquals("sub graph has two vertices", 1L, sub2.getVertexCount());
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData> sub3 =
      labeledGraphCollection.getGraph(-3L);
    assertEquals("Sub graph has no edge", 0L, sub3.getEdgeCount());
    assertEquals("sub graph has two vertices", 1L, sub3.getVertexCount());
    assertNotNull("graph collection is null", labeledGraphCollection);
    assertEquals("wrong number of graphs", 3l,
      labeledGraphCollection.getGraphCount());
    assertEquals("wrong number of vertices", 4l,
      labeledGraphCollection.getVertexCount());
    assertEquals("wrong number of edges", 0l,
      labeledGraphCollection.getEdgeCount());
  }
}
