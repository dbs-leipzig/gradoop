
package org.gradoop.model.impl.operators.collection.unary;

  import com.google.common.collect.Lists;
  import org.apache.flink.api.common.functions.MapFunction;
  import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
  import org.gradoop.model.FlinkTestBase;
  import org.gradoop.model.impl.LogicalGraph;
  import org.gradoop.model.impl.GraphCollection;
  import org.gradoop.model.impl.id.GradoopId;
  import org.gradoop.model.impl.id.GradoopIdSet;
  import org.gradoop.model.impl.pojo.EdgePojo;
  import org.gradoop.model.impl.pojo.GraphHeadPojo;
  import org.gradoop.model.impl.pojo.VertexPojo;
  import org.junit.Test;
  import org.junit.runner.RunWith;
  import org.junit.runners.Parameterized;

  import java.util.List;

  import static org.junit.Assert.assertEquals;
  import static org.junit.Assert.assertNotNull;
  import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GraphCollectionCombineTest extends FlinkTestBase {
  public GraphCollectionCombineTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void combineCollectionTest() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> coll =
      getGraphStore()
        .getCollection()
        .getGraphs(GradoopIdSet.fromLongs(1L, 2L, 3L));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      newGraph = coll.callForGraph(
      new CombineCollection<VertexPojo, EdgePojo, GraphHeadPojo>());


    List<VertexPojo> oldVertices = Lists.newArrayList();
    List<EdgePojo> oldEdges = Lists.newArrayList();
    List<GradoopId> oldGraphs = Lists.newArrayList();
    List<VertexPojo> newVertices = Lists.newArrayList();
    List<EdgePojo> newEdges = Lists.newArrayList();

    coll.getVertices().output(new LocalCollectionOutputFormat<>(oldVertices));
    coll.getEdges().output(new LocalCollectionOutputFormat<>(oldEdges));
    coll.getGraphHeads().map(new MapFunction<GraphHeadPojo, GradoopId>() {
      @Override
      public GradoopId map(GraphHeadPojo graphData) throws Exception {
        return graphData.getId();
      }
    }).output(new LocalCollectionOutputFormat<>(oldGraphs));

    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges()
      .output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", newGraph);
    assertEquals(oldVertices.size(), newVertices.size());
    assertEquals(oldEdges.size(), newEdges.size());
    
    for (VertexPojo vertex : newVertices) {
      assertTrue(oldVertices.contains(vertex));
    }
    for (EdgePojo edge : newEdges) {
      assertTrue(oldEdges.contains(edge));
    }
  }
}
