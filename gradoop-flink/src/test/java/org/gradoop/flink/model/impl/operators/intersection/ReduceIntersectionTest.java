package org.gradoop.flink.model.impl.operators.intersection;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class ReduceIntersectionTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col02 = loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection expectation = loader.getGraphCollectionByVariables("g2");

    GraphCollection result = col02.intersect(col12);
    checkAssertions(expectation, result, "");

    result = col02.intersectWithSmallResult(col12);
    checkAssertions(expectation, result, "small");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col01 = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection col23 = loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection expectation = GraphCollection.createEmptyCollection(config);

    GraphCollection result = col01.intersect(col23);

    checkAssertions(expectation, result, "non");

    result = col01.intersectWithSmallResult(col23);
    checkAssertions(expectation, result, "small non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection result = expectation.intersect(expectation);
    checkAssertions(expectation, result, "total");

    result = expectation.intersectWithSmallResult(expectation);
    checkAssertions(expectation, result, "small total");
  }
}
