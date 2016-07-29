package org.gradoop.model.impl.operators.union;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class UnionTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> col02 =
      loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> result =
      col02.union(col12);

    GraphCollection<GraphHead, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    checkAssertions(expectation, result, "");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> col01 =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> col23 =
      loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> result =
      col01.union(col23);

    GraphCollection<GraphHead, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    checkAssertions(expectation, result, "non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHead, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHead, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHead, VertexPojo, EdgePojo> result =
      expectation.union(expectation);

    checkAssertions(expectation, result, "total");
  }
}
