package org.gradoop.flink.model.impl.operators.sampling;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public abstract class ParametrizedTestForGraphSampling extends GradoopFlinkTestBase {

  private final String testName;

  protected final long seed;

  protected final float sampleSize;

  protected final Neighborhood.NeighborType neighborType;

  public ParametrizedTestForGraphSampling(String testName, String seed, String sampleSize,
                                             String neighborType) {
    this.testName = testName;
    this.seed = Long.parseLong(seed);
    this.sampleSize = Float.parseFloat(sampleSize);
    this.neighborType = Neighborhood.fromString(neighborType);
  }

  public abstract UnaryGraphToGraphOperator getSamplingOperator();

  @Test
  public void randomSamplingTest() throws Exception {
    LogicalGraph dbGraph = getSocialNetworkLoader().getDatabase().getDatabaseGraph();

    LogicalGraph newGraph = getSamplingOperator().execute(dbGraph); //dbGraph.sampleRandomNodes(0.272f);

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

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
            new String[] {
                    "With seed and both neighborhood",
                    "-4181668494294894490",
                    "0.272f",
                    "Both"
            },
            new String[] {
                    "Without seed and both neighborhood",
                    "-4181668494294894490",
                    "0",
                    "Both"
            },
            new String[] {
                    "With seed and input neighborhood",
                    "-4181668494294894490",
                    "0.272f",
                    "Input"
            },
            new String[] {
                    "With seed and output neighborhood",
                    "-4181668494294894490",
                    "0.272f",
                    "Output"
            });
  }
}
