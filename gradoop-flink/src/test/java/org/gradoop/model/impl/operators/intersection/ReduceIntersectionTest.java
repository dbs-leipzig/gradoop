package org.gradoop.model.impl.operators.intersection;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class ReduceIntersectionTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col02 =
      loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col12 =
      loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col02.intersect(col12);
    checkAssertions(expectation, result, "");

    result = col02.intersectWithSmallResult(col12);
    checkAssertions(expectation, result, "small");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col01 =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> col23 =
      loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      GraphCollection.createEmptyCollection(config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      col01.intersect(col23);
    checkAssertions(expectation, result, "non");

    result = col01.intersectWithSmallResult(col23);
    checkAssertions(expectation, result, "small non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> expectation =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> result =
      expectation.intersect(expectation);
    checkAssertions(expectation, result, "total");

    result = expectation.intersectWithSmallResult(expectation);
    checkAssertions(expectation, result, "small total");
  }
}
