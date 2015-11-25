package org.gradoop.model.impl.operators.collection.binary;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class DifferenceTest extends
  BinaryCollectionOperatorsTestBase {

  public DifferenceTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    // 0 1 2, 0, 2, 5, 8
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 5L;
    long expectedEdgeCount = 8L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L, 2L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L));

    GraphCollection differenceColl = collection1.difference(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);

    differenceColl = collection1.differenceWithSmallResult(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    // "0 1, 2 3, 2, 6, 8"
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 8L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(2L, 3L));

    GraphCollection differenceColl = collection1.difference(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);

    differenceColl = collection1.differenceWithSmallResult(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    // "0 1, 0 1, 0, 0, 0"
    long expectedCollectionSize = 0L;
    long expectedVertexCount = 0L;
    long expectedEdgeCount = 0L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L));

    GraphCollection differenceColl = collection1.difference(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);

    differenceColl = collection1.differenceWithSmallResult(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      differenceColl);
  }
}
