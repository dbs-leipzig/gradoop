package org.gradoop.model.impl.operators.collection.binary;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class GraphCollectionIntersectTest extends
  BinaryCollectionOperatorsTestBase {

  public GraphCollectionIntersectTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 6L;
    long expectedEdgeCount = 10L;
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L, 1L, 2L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(0L, 1L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
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
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L, 2L, 3L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(1L, 2L, 3L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
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
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(1L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
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
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L, 2L, 3L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(0L, 2L, 3L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }
}
