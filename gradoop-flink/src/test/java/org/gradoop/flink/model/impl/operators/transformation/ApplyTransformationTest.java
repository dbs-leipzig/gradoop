package org.gradoop.flink.model.impl.operators.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public class ApplyTransformationTest extends TransformationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testMissingFunctions() {
    new ApplyTransformation(null, null, null);
  }

  @Test
  public void testIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    List<GradoopId> expectedGraphHeadIds  = Lists.newArrayList();
    List<GradoopId> expectedVertexIds     = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds       = Lists.newArrayList();

    inputCollection.getGraphHeads().map(new Id<GraphHead>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputCollection.getVertices().map(new Id<Vertex>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputCollection.getEdges().map(new Id<Edge>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(
        new GraphHeadModifier(),
        new VertexModifier(),
        new EdgeModifier()));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds    = Lists.newArrayList();
    List<GradoopId> resultEdgeIds      = Lists.newArrayList();

    outputCollection.getGraphHeads().map(new Id<GraphHead>()).output(
      new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    outputCollection.getVertices().map(new Id<Vertex>()).output(
      new LocalCollectionOutputFormat<>(resultVertexIds));
    outputCollection.getEdges().map(new Id<Edge>()).output(
      new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    GradoopTestUtils.validateIdEquality(
      expectedGraphHeadIds,
      resultGraphHeadIds);
    GradoopTestUtils.validateIdEquality(
      expectedVertexIds,
      resultVertexIds);
    GradoopTestUtils.validateIdEquality(
      expectedEdgeIds,
      resultEdgeIds);
  }

  @Test
  public void testDataEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g01", "g11");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(
        new GraphHeadModifier(),
        new VertexModifier(),
        new EdgeModifier()));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testGraphHeadOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection =
      loader.getGraphCollectionByVariables("g02", "g12");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(new GraphHeadModifier(), null, null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testVertexOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g03", "g13");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(null, new VertexModifier(), null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g04", "g14");

    GraphCollection outputCollection = inputCollection.apply(
        new ApplyTransformation(null, null, new EdgeModifier()));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }
}
