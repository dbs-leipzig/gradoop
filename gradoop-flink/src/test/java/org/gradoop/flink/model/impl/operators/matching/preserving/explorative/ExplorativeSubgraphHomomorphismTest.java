package org.gradoop.flink.model.impl.operators.matching.preserving.explorative;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.preserving.SubgraphHomomorphismTest;

public class ExplorativeSubgraphHomomorphismTest extends
        SubgraphHomomorphismTest {

  public ExplorativeSubgraphHomomorphismTest(String testName, String dataGraph,
                                             String queryGraph, String[] expectedGraphVariables,
                                             String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    return new ExplorativePatternMatching(queryGraph, attachData, MatchStrategy.HOMOMORPHISM );
  }
}
