package org.gradoop.flink.model.impl.operators.difference;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DifferenceTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col02 = loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection expectation = loader.getGraphCollectionByVariables("g0");

    GraphCollection result = col02.difference(col12);
    checkAssertions(expectation, result, "");

    result = col02.differenceWithSmallResult(col12);
    checkAssertions(expectation, result, "small");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col01 = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection col23 = loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection result = col01.difference(col23);
    checkAssertions(col01, result, "non");

    result = col01.differenceWithSmallResult(col23);

    checkAssertions(col01, result, "small non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col01 = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectation = GraphCollection.createEmptyCollection(config);

    GraphCollection result = col01.difference(col01);
    checkAssertions(expectation, result, "total");

    result = col01.differenceWithSmallResult(col01);
    checkAssertions(expectation, result, "small total");
  }
}
