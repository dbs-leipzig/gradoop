package org.gradoop.model.impl.operators.collection.unary;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class GraphCollectionExcludeTest extends FlinkTestBase {
  public GraphCollectionExcludeTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void overlapCollectionTest() throws Exception {
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo> coll =
      getGraphStore().getCollection().getGraphs(
        GradoopId.fromLong(1L),
        GradoopId.fromLong(2L),
        GradoopId.fromLong(3L));
    GradoopId exclusionBase = GradoopId.fromLong(1L);
    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      newGraph = coll.callForGraph(
      new ExcludeCollection<VertexPojo, EdgePojo, GraphHeadPojo>(GradoopId
        .fromLong(1L)));

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
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", newGraph);
    for (VertexPojo vertex : newVertices) {
      assertTrue(vertex.getGraphIds().contains(exclusionBase));
      for(GradoopId id : oldGraphs){
        if(!id.equals(exclusionBase)){
          assertFalse(vertex.getGraphIds().contains(id));
        }
      }
      assertTrue(oldGraphs.containsAll(vertex.getGraphIds().toCollection()));
    }
    for (EdgePojo edge : newEdges) {
      assertTrue(oldEdges.contains(edge));
      assertTrue(edge.getGraphIds().contains(exclusionBase));
      for(GradoopId id : oldGraphs){
        if(!id.equals(exclusionBase)){
          assertFalse(edge.getGraphIds().contains(id));
        }
      }
      assertTrue(oldGraphs.containsAll(edge.getGraphIds().toCollection()));
    }
  }
}
