package org.gradoop.model.impl;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class GraphCollectionUnionTest extends
  BinaryCollectionOperatorsTestBase {

  public GraphCollectionUnionTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void testOverlappingCollections() throws Exception {
    long expectedCollectionSize = 2L;
    long expectedVertexCount = 5L;
    long expectedEdgeCount = 8L;
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L, 2L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(2L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    long expectedCollectionSize = 4L;
    long expectedVertexCount = 7L;
    long expectedEdgeCount = 13L;
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L, 1L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(2L, 3L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    long expectedCollectionSize = 1L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      graphColl = graphStore.getCollection();
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection1 = graphColl.getGraphs(0L);
    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      collection2 = graphColl.getGraphs(0L);

    GraphCollection<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }
}
