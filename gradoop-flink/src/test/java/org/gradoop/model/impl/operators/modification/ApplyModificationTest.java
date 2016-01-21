package org.gradoop.model.impl.operators.modification;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.GradoopTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.modification.functions.ModifyGraphHead;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public class ApplyModificationTest extends ModificationTest {

  @Test
  public void testElementIdentity() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    List<GradoopId> expectedGraphHeadIds  = Lists.newArrayList();
    List<GradoopId> expectedVertexIds     = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds       = Lists.newArrayList();

    inputCollection.getGraphHeads().map(new Id<GraphHeadPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputCollection.getVertices().map(new Id<VertexPojo>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputCollection.getEdges().map(new Id<EdgePojo>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(new ApplyModification<>(
        new GraphHeadModifier<GraphHeadPojo>(),
        new VertexModifier<VertexPojo>(),
        new EdgeModifier<EdgePojo>()));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds    = Lists.newArrayList();
    List<GradoopId> resultEdgeIds      = Lists.newArrayList();

    outputCollection.getGraphHeads().map(new Id<GraphHeadPojo>()).output(
      new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    outputCollection.getVertices().map(new Id<VertexPojo>()).output(
      new LocalCollectionOutputFormat<>(resultVertexIds));
    outputCollection.getEdges().map(new Id<EdgePojo>()).output(
      new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    GradoopTestUtils.validateIdLists(expectedGraphHeadIds, resultGraphHeadIds);
    GradoopTestUtils.validateIdLists(expectedVertexIds, resultVertexIds);
    GradoopTestUtils.validateIdLists(expectedEdgeIds, resultEdgeIds);
  }

  @Test
  public void testElementEquality() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(TEST_GRAPH);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectedCollection =
      loader.getGraphCollectionByVariables("g01", "g11");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> outputCollection =
      inputCollection.apply(new ApplyModification<>(
        new GraphHeadModifier<GraphHeadPojo>(),
        new VertexModifier<VertexPojo>(),
        new EdgeModifier<EdgePojo>()));

    collectAndAssertTrue(
      outputCollection.equalsByGraphElementData(expectedCollection));
  }
}
