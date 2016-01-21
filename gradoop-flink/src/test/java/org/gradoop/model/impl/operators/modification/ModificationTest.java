package org.gradoop.model.impl.operators.modification;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.ModificationFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.gradoop.GradoopTestUtils.validateIdLists;
import static org.junit.Assert.assertEquals;

public class ModificationTest extends GradoopFlinkTestBase {

  protected static final String TEST_GRAPH = "" +
    "g0:A  { a = 1 } [(:A { a = 1, b = 2 })-[:a { a = 1, b = 2 }]->(:B { c = 2 })]" +
    "g1:B  { a = 2 } [(:A { a = 2, b = 2 })-[:a { a = 2, b = 2 }]->(:B { c = 3 })]" +
    "g01:A { a = 2 } [(:A { a = 2, b = 1 })-->(:B { d = 2 })]" +
    "g11:B { a = 3 } [(:A { a = 3, b = 1 })-->(:B { d = 3 })]";

  /**
   * Tests if the identifiers of the resulting elements are the same as in the
   * input graph.
   *
   * @throws Exception
   */
  @Test
  public void testElementIdentity() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    List<GradoopId> expectedGraphHeadIds = Lists.newArrayList();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> inputGraph =
      loader.getLogicalGraphByVariable("g0");

    inputGraph.getGraphHead().map(new Id<GraphHeadPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputGraph.getVertices().map(new Id<VertexPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputGraph.getEdges().map(new Id<EdgePojo>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> result = inputGraph
      .modify(
        new GraphHeadModifier<GraphHeadPojo>(),
        new VertexModifier<VertexPojo>(),
        new EdgeModifier<EdgePojo>()
      );

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds = Lists.newArrayList();
    List<GradoopId> resultEdgeIds = Lists.newArrayList();

    result.getGraphHead()
      .map(new Id<GraphHeadPojo>())
      .output(new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    result.getVertices()
      .map(new Id<VertexPojo>())
      .output(new LocalCollectionOutputFormat<>(resultVertexIds));
    result.getEdges()
      .map(new Id<EdgePojo>())
      .output(new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    validateIdLists(expectedGraphHeadIds, resultGraphHeadIds);
    validateIdLists(expectedVertexIds, resultVertexIds);
    validateIdLists(expectedEdgeIds, resultEdgeIds);
  }


  /**
   * Tests the data in the resulting graph.
   *
   * @throws Exception
   */
  @Test
  public void testElementEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> original = loader
      .getLogicalGraphByVariable("g0");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("g01");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      result = original.modify(
      new GraphHeadModifier<GraphHeadPojo>(),
      new VertexModifier<VertexPojo>(),
      new EdgeModifier<EdgePojo>()
    );

    // TODO: test graph head equality
    collectAndAssertTrue(result.equalsByElementData(expected));
  }

  public static class GraphHeadModifier<G extends EPGMGraphHead>
    implements ModificationFunction<G> {

    @Override
    public G execute(G current, G modified) {
      modified.setLabel(current.getLabel());
      modified.setProperty("a", current.getPropertyValue("a").getInt() + 1L);
      return modified;
    }
  }

  public static class VertexModifier<V extends EPGMVertex>
    implements ModificationFunction<V> {

    @Override
    public V execute(V current, V modified) {
      modified.setLabel(current.getLabel());
      if (current.getLabel().equals("A")) {
        modified.setProperty("a", current.getPropertyValue("a").getInt() + 1);
        modified.setProperty("b", current.getPropertyValue("b").getInt() - 1);
      } else if (current.getLabel().equals("B")) {
        modified.setProperty("d", current.getPropertyValue("c"));
      }
      return modified;
    }
  }

  public static class EdgeModifier<E extends EPGMEdge>
    implements ModificationFunction<E> {

    @Override
    public E execute(E current, E modified) {
      return modified;
    }
  }
}
