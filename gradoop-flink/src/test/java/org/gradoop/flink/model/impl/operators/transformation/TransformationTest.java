package org.gradoop.flink.model.impl.operators.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateIdEquality;

public class TransformationTest extends GradoopFlinkTestBase {

  protected static final String TEST_GRAPH = "" +
    "g0:A  { a = 1 } [(:A { a = 1, b = 2 })-[:a { a = 1, b = 2 }]->(:B { c = 2 })]" +
    "g1:B  { a = 2 } [(:A { a = 2, b = 2 })-[:a { a = 2, b = 2 }]->(:B { c = 3 })]" +
    // full graph transformation
    "g01:A { a = 2 } [(:A { a = 2, b = 1 })-->(:B { d = 2 })]" +
    "g11:B { a = 3 } [(:A { a = 3, b = 1 })-->(:B { d = 3 })]" +
    // graph head only transformation
    "g02:A { a = 2 } [(:A { a = 1, b = 2 })-[:a { a = 1, b = 2 }]->(:B { c = 2 })]" +
    "g12:B { a = 3 } [(:A { a = 2, b = 2 })-[:a { a = 2, b = 2 }]->(:B { c = 3 })]" +
    // vertex only transformation
    "g03:A { a = 1 } [(:A { a = 2, b = 1 })-[:a { a = 1, b = 2 }]->(:B { d = 2 })]" +
    "g13:B { a = 2 } [(:A { a = 3, b = 1 })-[:a { a = 2, b = 2 }]->(:B { d = 3 })]" +
    // edge only transformation
    "g04:A { a = 1 } [(:A { a = 1, b = 2 })-->(:B { c = 2 })]" +
    "g14:B { a = 2 } [(:A { a = 2, b = 2 })-->(:B { c = 3 })]";

  @Test(expected = IllegalArgumentException.class)
  public void testMissingFunctions() {
    new Transformation(null, null, null);
  }

  /**
   * Tests if the identifiers of the resulting elements are the same as in the
   * input graph.
   *
   * @throws Exception
   */
  @Test
  public void testIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    List<GradoopId> expectedGraphHeadIds = Lists.newArrayList();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();

    LogicalGraph inputGraph = loader.getLogicalGraphByVariable("g0");

    inputGraph.getGraphHead().map(new Id<GraphHead>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputGraph.getVertices().map(new Id<Vertex>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputGraph.getEdges().map(new Id<Edge>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    LogicalGraph result = inputGraph
      .transform(
        new GraphHeadModifier(),
        new VertexModifier(),
        new EdgeModifier()
      );

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds = Lists.newArrayList();
    List<GradoopId> resultEdgeIds = Lists.newArrayList();

    result.getGraphHead()
      .map(new Id<GraphHead>())
      .output(new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    result.getVertices()
      .map(new Id<Vertex>())
      .output(new LocalCollectionOutputFormat<>(resultVertexIds));
    result.getEdges()
      .map(new Id<Edge>())
      .output(new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    validateIdEquality(expectedGraphHeadIds, resultGraphHeadIds);
    validateIdEquality(expectedVertexIds, resultVertexIds);
    validateIdEquality(expectedEdgeIds, resultEdgeIds);
  }


  /**
   * Tests the data in the resulting graph.
   *
   * @throws Exception
   */
  @Test
  public void testDataEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g01");

    LogicalGraph
      result = original.transform(
      new GraphHeadModifier(),
      new VertexModifier(),
      new EdgeModifier()
    );

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testGraphHeadOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g02");

    LogicalGraph result = original.transformGraphHead(new GraphHeadModifier());

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testVertexOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g03");

    LogicalGraph result = original.transformVertices(new VertexModifier());

    collectAndAssertTrue(result.equalsByData(expected));
  }

  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    LogicalGraph original = loader.getLogicalGraphByVariable("g0");

    LogicalGraph expected = loader.getLogicalGraphByVariable("g04");

    LogicalGraph result = original.transformEdges(new EdgeModifier());

    collectAndAssertTrue(result.equalsByData(expected));
  }

  public static class GraphHeadModifier implements TransformationFunction<GraphHead> {

    @Override
    public GraphHead execute(GraphHead current, GraphHead transformed) {
      transformed.setLabel(current.getLabel());
      transformed.setProperty("a", current.getPropertyValue("a").getInt() + 1);
      return transformed;
    }
  }

  public static class VertexModifier implements TransformationFunction<Vertex> {

    @Override
    public Vertex execute(Vertex current, Vertex transformed) {
      transformed.setLabel(current.getLabel());
      if (current.getLabel().equals("A")) {
        transformed.setProperty("a", current.getPropertyValue("a").getInt() + 1);
        transformed.setProperty("b", current.getPropertyValue("b").getInt() - 1);
      } else if (current.getLabel().equals("B")) {
        transformed.setProperty("d", current.getPropertyValue("c"));
      }
      return transformed;
    }
  }

  public static class EdgeModifier implements TransformationFunction<Edge> {

    @Override
    public Edge execute(Edge current, Edge transformed) {
      return transformed;
    }
  }
}
