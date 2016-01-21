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

  private static String TEST_GRAPH = "" +
    "org:A{k=0}[" +
    "(:A {k=0, l=0})-[:a {l=1}]->(:B {l=1, m=2})" +
    "];" +
    "exp:A{k=1}[" +
    "(:A {k=1})-[]->(:B {l=1, n=2})" +
    "]";

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

    GradoopId expectedGraphHeadId = loader.getGraphHeadByVariable("org").getId();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    for (VertexPojo v : loader.getVerticesByGraphVariables("org")) {
      expectedVertexIds.add(v.getId());
    }
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();
    for (EdgePojo e : loader.getEdgesByGraphVariables("org")) {
      expectedEdgeIds.add(e.getId());
    }

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> result =
      loader.getLogicalGraphByVariable("org")
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

    assertEquals(expectedGraphHeadId, resultGraphHeadIds.get(0));

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
      .getLogicalGraphByVariable("org");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expected = loader
      .getLogicalGraphByVariable("exp");

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
    public G execute(G gOld, G gNew) throws Exception {
      gNew.setLabel(gOld.getLabel());
      gNew.setProperty("k", gOld.getPropertyValue("k").getInt() + 1L);
      return gNew;
    }
  }

  public static class VertexModifier<V extends EPGMVertex>
    implements ModificationFunction<V> {

    @Override
    public V execute(V vOld, V vNew) throws Exception {
      vNew.setLabel(vOld.getLabel());
      if (vOld.getLabel().equals("B")) {
        vNew.setProperty("l", vOld.getPropertyValue("l"));
        vNew.setProperty("n", vOld.getPropertyValue("m"));
      } else {
        vNew.setProperty("k", vOld.getPropertyValue("k").getInt() + 1L);
      }
      return vNew;
    }
  }

  public static class EdgeModifier<E extends EPGMEdge>
    implements ModificationFunction<E> {

    @Override
    public E execute(E eOld, E eNew) throws Exception {
      return eNew;
    }
  }

}
