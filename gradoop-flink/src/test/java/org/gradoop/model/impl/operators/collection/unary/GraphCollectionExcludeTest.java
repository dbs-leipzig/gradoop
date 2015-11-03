package org.gradoop.model.impl.operators.collection.unary;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.collection.unary.ExcludeCollection;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
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
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData> coll =
      getGraphStore().getCollection().getGraphs(1L, 2L, 3L);
    Long exclusionBase = 1L;
    LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      newGraph = coll.callForGraph(
      new ExcludeCollection<DefaultVertexData, DefaultEdgeData,
              DefaultGraphData>(
        1L));

    List<DefaultVertexData> oldVertices = Lists.newArrayList();
    List<DefaultEdgeData> oldEdges = Lists.newArrayList();
    List<Long> oldGraphs = Lists.newArrayList();
    List<DefaultVertexData> newVertices = Lists.newArrayList();
    List<DefaultEdgeData> newEdges = Lists.newArrayList();

    coll.getVertices().output(new LocalCollectionOutputFormat<>(oldVertices));
    coll.getEdges().output(new LocalCollectionOutputFormat<>(oldEdges));
    coll.getGraphHeads().map(new MapFunction<DefaultGraphData, Long>() {
      @Override
      public Long map(DefaultGraphData graphData) throws Exception {
        return graphData.getId();
      }
    }).output(new LocalCollectionOutputFormat<>(oldGraphs));
    newGraph.getVertices()
      .output(new LocalCollectionOutputFormat<>(newVertices));
    newGraph.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", newGraph);
    for (DefaultVertexData vertex : newVertices) {
      assertTrue(vertex.getGraphs().contains(exclusionBase));
      for(Long id : oldGraphs){
        if(!id.equals(exclusionBase)){
          assertFalse(vertex.getGraphs().contains(id));
        }
      }
      assertTrue(oldGraphs.containsAll(vertex.getGraphs()));
    }
    for (DefaultEdgeData edge : newEdges) {
      assertTrue(oldEdges.contains(edge));
      assertTrue(edge.getGraphs().contains(exclusionBase));
      for(Long id : oldGraphs){
        if(!id.equals(exclusionBase)){
          assertFalse(edge.getGraphs().contains(id));
        }
      }
      assertTrue(oldGraphs.containsAll(edge.getGraphs()));
    }
  }
}
