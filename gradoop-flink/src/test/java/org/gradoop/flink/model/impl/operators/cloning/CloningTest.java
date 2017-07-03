
package org.gradoop.flink.model.impl.operators.cloning;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.IdAsIdSet;
import org.gradoop.flink.model.impl.functions.graphcontainment.ExpandGraphsToIdSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.flink.model.impl.functions.epgm.IdSetCombiner;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateIdInequality;
import static org.junit.Assert.assertTrue;

public class CloningTest extends GradoopFlinkTestBase {

  @Test
  public void testCloning() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(
        "org:Ga{k : 0}[(:Va{k : 0, l : 0})-[:ea{l : 1}]->(:Va{l : 1, m : 2})]"
      );

    List<GradoopId> expectedGraphHeadIds = Lists.newArrayList();
    List<GradoopId> expectedVertexIds = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds = Lists.newArrayList();


    LogicalGraph original = loader.getLogicalGraphByVariable("org");

    original.getGraphHead().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    original.getVertices().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    original.getEdges().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));


    LogicalGraph result = original.copy();

    collectAndAssertTrue(result.equalsByElementData(original));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds = Lists.newArrayList();
    List<GradoopId> resultEdgeIds = Lists.newArrayList();

    result.getGraphHead()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    result.getVertices()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultVertexIds));
    result.getEdges()
      .map(new Id<>())
      .output(new LocalCollectionOutputFormat<>(resultEdgeIds));


    List<GradoopIdList> resultGraphIds = Lists.newArrayList();

    result.getVertices()
      .map(new ExpandGraphsToIdSet<>())
      .union(result.getEdges()
        .map(new ExpandGraphsToIdSet<>()))
      .union(result.getGraphHead()
        .map(new IdAsIdSet<>()))
      .reduce(new IdSetCombiner())
      .output(new LocalCollectionOutputFormat<>(resultGraphIds));

    getExecutionEnvironment().execute();

    assertTrue("elements in multiple graphs",
      resultGraphIds.size() == 1);

    assertTrue("wrong number of graph heads",
      expectedGraphHeadIds.size() == resultGraphHeadIds.size());

    assertTrue("wrong number of vertices",
      expectedVertexIds.size() == resultVertexIds.size());

    assertTrue("wrong number of edges",
      expectedEdgeIds.size() == resultEdgeIds.size());


    validateIdInequality(expectedGraphHeadIds, resultGraphHeadIds);
    validateIdInequality(expectedVertexIds, resultVertexIds);
    validateIdInequality(expectedEdgeIds, resultEdgeIds);

  }
}
