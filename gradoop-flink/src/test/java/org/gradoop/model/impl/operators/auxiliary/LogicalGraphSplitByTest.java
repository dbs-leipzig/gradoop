//package org.gradoop.model.impl.operators.auxiliary;
//
//import com.google.common.collect.Lists;
//import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
//import org.gradoop.model.FlinkTestBase;
//import org.gradoop.model.impl.LogicalGraph;
//import org.gradoop.model.impl.GraphCollection;
//import org.gradoop.model.impl.functions.UnaryFunction;
//import org.gradoop.model.impl.id.GradoopId;
//import org.gradoop.model.impl.pojo.EdgePojo;
//import org.gradoop.model.impl.pojo.GraphHeadPojo;
//import org.gradoop.model.impl.pojo.VertexPojo;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.util.List;
//
//import static org.junit.Assert.*;
//
//@RunWith(Parameterized.class)
//public class LogicalGraphSplitByTest extends FlinkTestBase {
//  public LogicalGraphSplitByTest(TestExecutionMode mode) {
//    super(mode);
//  }
//
//  @Test
//  public void testSplitBy() throws Exception {
//    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
//      inputGraph = getGraphStore().getGraph(GradoopId.fromLong(0L));
//    UnaryFunction<VertexPojo, GradoopId> splitFunc = new SplitByIdOddOrEven();
//
//    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
//      labeledGraphCollection = inputGraph.callForCollection(
//      new SplitBy<VertexPojo, EdgePojo, GraphHeadPojo>(
//        splitFunc,
//        getExecutionEnvironment()));
//
//    List<VertexPojo> oldVertices = Lists.newArrayList();
//    inputGraph.getVertices()
//      .output(new LocalCollectionOutputFormat<>(oldVertices));
//
//    List<VertexPojo> newVertices = Lists.newArrayList();
//    labeledGraphCollection.getVertices()
//      .output(new LocalCollectionOutputFormat<>(newVertices));
//
//    List<EdgePojo> newEdges = Lists.newArrayList();
//    labeledGraphCollection.getEdges()
//      .output(new LocalCollectionOutputFormat<>(newEdges));
//
//    assertNotNull("graph collection is null", labeledGraphCollection);
//    assertEquals("wrong number of graphs", 2L,
//      labeledGraphCollection.getGraphCount());
//    assertEquals("wrong number of vertices", 3L, newVertices.size());
//    assertEquals("wrong number of edges", 1L, newEdges.size());
//
//    for (int i = 0; i < newVertices.size(); i++) {
//      VertexPojo oldVertex = oldVertices.get(i);
//      VertexPojo newVertex = newVertices.get(i);
//      assertTrue((oldVertex.getGraphCount() + 1) == newVertex.getGraphCount());
//      assertTrue(newVertex.getGraphIds().containsAll(oldVertex.getGraphIds()));
//      assertTrue(newVertex.getGraphIds().contains(splitFunc.execute(newVertex)));
//    }
//  }
//
//  private static class SplitByIdOddOrEven implements
//    UnaryFunction<VertexPojo, GradoopId> {
//    @Override
//    public GradoopId execute(VertexPojo entity) throws
//      Exception {
//      // TODO return (entity.getId() % 2) - 2;
//      return new GradoopId();
//    }
//  }
//}
