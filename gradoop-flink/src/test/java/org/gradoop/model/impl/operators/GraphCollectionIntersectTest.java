package org.gradoop.model.impl.operators;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultVertexData;
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
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = getGraphStore().getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L, 1L, 2L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(0L, 1L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
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
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = getGraphStore().getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L, 2L, 3L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(1L, 2L, 3L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
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
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = getGraphStore().getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(1L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
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
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = getGraphStore().getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L, 2L, 3L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(0L, 2L, 3L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      intersectColl = collection1.intersect(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
    intersectColl = collection1.intersectWithSmall(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      intersectColl);
  }
}
