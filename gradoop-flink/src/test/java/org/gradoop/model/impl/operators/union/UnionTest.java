package org.gradoop.model.impl.operators.union;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.base.BinaryCollectionOperatorsTestBase;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class UnionTest extends BinaryCollectionOperatorsTestBase {

  @Test
  public void testOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col02 = loader.getGraphCollectionByVariables("g0", "g2");

    GraphCollection col12 = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection result = col02.union(col12);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2");

    checkAssertions(expectation, result, "");
  }

  @Test
  public void testNonOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection col01 = loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection col23 = loader.getGraphCollectionByVariables("g2", "g3");

    GraphCollection result = col01.union(col23);

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    checkAssertions(expectation, result, "non");
  }

  @Test
  public void testTotalOverlappingCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection result = expectation.union(expectation);

    checkAssertions(expectation, result, "total");
  }
}
