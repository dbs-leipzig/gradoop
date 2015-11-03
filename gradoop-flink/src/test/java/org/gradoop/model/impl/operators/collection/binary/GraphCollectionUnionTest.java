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
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L, 2L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(2L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    long expectedCollectionSize = 4L;
    long expectedVertexCount = 7L;
    long expectedEdgeCount = 13L;
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L, 1L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(2L, 3L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    long expectedCollectionSize = 1L;
    long expectedVertexCount = 3L;
    long expectedEdgeCount = 4L;
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      graphColl = getGraphStore().getCollection();
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection1 = graphColl.getGraphs(0L);
    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      collection2 = graphColl.getGraphs(0L);

    GraphCollection<VertexPojo, EdgePojo, GraphHeadPojo>
      unionColl = collection1.union(collection2);

    performTest(expectedCollectionSize, expectedVertexCount, expectedEdgeCount,
      unionColl);
  }
}
