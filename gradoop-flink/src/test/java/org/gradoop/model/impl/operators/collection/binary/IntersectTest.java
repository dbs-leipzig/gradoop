package org.gradoop.model.impl.operators.collection.binary;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IntersectTest extends
  BinaryCollectionOperatorsTestBase {

  public IntersectTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 10L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L, 2L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 1L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }

  @Test
  public void testOverlappingCollections2() throws Exception {
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 5L;
    long expectedEdgeCount = 9L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 2L, 3L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(1L, 2L, 3L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    long expectedCollectionSize = 0L;
    long expectedVertexCount = 0L;
    long expectedEdgeCount = 0L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(1L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);

    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    long expectedCollectionSize = 3L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 11L;
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection1 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 2L, 3L));
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collection2 = graphColl.getGraphs(GradoopIdSet.fromLongs(0L, 2L, 3L));

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }
}
