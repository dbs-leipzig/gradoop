package org.gradoop.flink.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

//public abstract class BasicPatternsTransactionalFSMTestBase extends GradoopFlinkTestBase {

@RunWith(Parameterized.class)
public abstract class ParametrizedTestForGraphSampling extends GradoopFlinkTestBase {
  public abstract UnaryGraphToGraphOperator getSamplingOperator();

  public abstract UnaryGraphToGraphOperator getSamplingWithSeedOperator();

  @Test
  public void randomSamplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = getSamplingOperator().execute(dbGraph); //dbGraph.sampleRandomNodes(0.272f);

    validateResult(dbGraph, newGraph);
  }

  @Test
  public void randomSamplingTestWithSeed() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader()
            .getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = getSamplingOperator().execute(dbGraph);

    validateResult(dbGraph, newGraph);
  }

  private void validateResult(LogicalGraph input, LogicalGraph output)
          throws Exception {
    List<Vertex> dbVertices = Lists.newArrayList();
    List<Edge> dbEdges = Lists.newArrayList();
    List<Vertex> newVertices = Lists.newArrayList();
    List<Edge> newEdges = Lists.newArrayList();

    input.getVertices().output(new LocalCollectionOutputFormat<>(dbVertices));
    input.getEdges().output(new LocalCollectionOutputFormat<>(dbEdges));

    output.getVertices().output(new LocalCollectionOutputFormat<>(newVertices));
    output.getEdges().output(new LocalCollectionOutputFormat<>(newEdges));

    getExecutionEnvironment().execute();

    assertNotNull("graph was null", output);

    Set<GradoopId> newVertexIDs = new HashSet<>();
    for (Vertex vertex : newVertices) {
      assertTrue(dbVertices.contains(vertex));
      newVertexIDs.add(vertex.getId());
    }
    for (Edge edge : newEdges) {
      assertTrue(dbEdges.contains(edge));
      assertTrue(newVertexIDs.contains(edge.getSourceId()));
      assertTrue(newVertexIDs.contains(edge.getTargetId()));
    }
    dbEdges.removeAll(newEdges);
    for (Edge edge : dbEdges) {
      assertFalse(
              newVertexIDs.contains(edge.getSourceId()) &&
                      newVertexIDs.contains(edge.getTargetId()));
    }
  }
}
