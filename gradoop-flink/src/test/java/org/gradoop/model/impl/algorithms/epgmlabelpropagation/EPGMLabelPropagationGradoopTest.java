//package org.gradoop.model.impl.algorithms.epgmlabelpropagation;
//
//import org.gradoop.model.FlinkTestBase;
//import org.gradoop.model.impl.LogicalGraph;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.pojo.EdgePojo;
//import org.gradoop.model.impl.pojo.GraphHeadPojo;
//import org.gradoop.model.impl.pojo.VertexPojo;
//import org.gradoop.model.impl.GraphCollection;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import static junit.framework.TestCase.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
///**
// * Tests the algorithm using the Gradoop Graph.callForCollection() method.
// */
//@RunWith(Parameterized.class)
//public class EPGMLabelPropagationGradoopTest extends FlinkTestBase {
//
//  final String propertyKey = EPGMLabelPropagationAlgorithm.CURRENT_VALUE;
//
//  public EPGMLabelPropagationGradoopTest(TestExecutionMode mode) {
//    super(mode);
//  }
//
//  @Test
//  public void testLabelPropagationWithCallByPropertyKey() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getGraph(GradoopId.fromLong(2L));
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
//      labeledGraphCollection = inputGraph.callForCollection(
//      new EPGMLabelPropagation<VertexPojo, EdgePojo, GraphHeadPojo>(2, propertyKey, getExecutionEnvironment()));
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> sub1 =
//      labeledGraphCollection.getGraph(GradoopId.fromLong(-2L));
//    assertEquals("Sub graph has no edge", 0L, sub1.getEdgeCount());
//    assertEquals("sub graph has two vertices", 2L, sub1.getVertexCount());
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> sub2 =
//      labeledGraphCollection.getGraph(GradoopId.fromLong(-1L));
//    assertEquals("Sub graph has no edge", 0L, sub2.getEdgeCount());
//    assertEquals("sub graph has two vertices", 1L, sub2.getVertexCount());
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo> sub3 =
//      labeledGraphCollection.getGraph(GradoopId.fromLong(-3L));
//    assertEquals("Sub graph has no edge", 0L, sub3.getEdgeCount());
//    assertEquals("sub graph has two vertices", 1L, sub3.getVertexCount());
//    assertNotNull("graph collection is null", labeledGraphCollection);
//    assertEquals("wrong number of graphs", 3l,
//      labeledGraphCollection.getGraphCount());
//    assertEquals("wrong number of vertices", 4l,
//      labeledGraphCollection.getVertexCount());
//    assertEquals("wrong number of edges", 0l,
//      labeledGraphCollection.getEdgeCount());
//  }
//}
