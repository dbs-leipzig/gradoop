//package org.gradoop.model.impl.operators.auxiliary;
//
//import junit.framework.TestCase;
//import org.gradoop.model.FlinkTestBase;
//import org.gradoop.model.impl.GraphCollection;
//import org.gradoop.model.impl.LogicalGraph;
//import org.gradoop.model.impl.functions.UnaryFunction;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.pojo.EdgePojo;
//import org.gradoop.model.impl.pojo.GraphHeadPojo;
//import org.gradoop.model.impl.pojo.VertexPojo;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.assertNotNull;
//
//@RunWith(Parameterized.class)
//public class LogicalGraphOverlapSplitByTest extends FlinkTestBase {
//  public LogicalGraphOverlapSplitByTest(TestExecutionMode mode) {
//    super(mode);
//  }
//
//  @Test
//  public void testOverlappingResultGraphs() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getDatabaseGraph();
//    UnaryFunction<VertexPojo, List<GradoopId>> function =
//      new SplitByModulo();
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
//      labeledGraphCollection = inputGraph.callForCollection(
//      new OverlapSplitBy<VertexPojo, EdgePojo, GraphHeadPojo>(
//        function));
//    labeledGraphCollection.getGraphHeads();
//    assertNotNull("graph collection is null", labeledGraphCollection);
//    TestCase.assertEquals("wrong number of graphs", 3l,
//      labeledGraphCollection.getGraphCount());
//    TestCase.assertEquals("wrong number of vertices", 11l,
//      labeledGraphCollection.getVertexCount());
//    TestCase.assertEquals("wrong number of edges", 8l,
//      labeledGraphCollection.getEdgeCount());
//  }
//
//  private static class SplitByModulo implements
//    UnaryFunction<VertexPojo, List<GradoopId>> {
//    @Override
//    public List<GradoopId> execute(VertexPojo vertex) throws
//      Exception {
//      List<GradoopId> list = new ArrayList<>();
//      boolean inNewGraph = false;
//
///*
//      TODO:
//      if (vertex.getId() % 2 == 0) {
//        list.add(-2l);
//        inNewGraph = true;
//      }
//      if (vertex.getId() % 3 == 0) {
//        list.add(-3l);
//        inNewGraph = true;
//      }
//      if (!inNewGraph) {
//        list.add(-1l);
//      }*/
//      return list;
//    }
//  }
//}
