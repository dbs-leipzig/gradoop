package org.gradoop.model.impl.operators.collection.binary;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class UnionTest extends
  BinaryCollectionOperatorsTestBase {

  public UnionTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col02 =
      loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col02.union(col12);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    assertTrue("wrong graph ids after union of overlapping collections",
      result.equalsByGraphIdsCollected(expectation));
    assertTrue("wrong graph element ids after union of overlapping collections",
      result.equalsByGraphElementIdsCollected(expectation));
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    long expectedCollectionSize = 4L;
    long expectedVertexCount = 7L;
    long expectedEdgeCount = 13L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(2L, 3L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    long expectedCollectionSize = 1L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }
}
