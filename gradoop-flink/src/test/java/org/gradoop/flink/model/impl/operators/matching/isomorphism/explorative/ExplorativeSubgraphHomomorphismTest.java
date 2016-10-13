package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.SubgraphHomomorphismTest;

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
    return new ExplorativeSubgraphIsomorphism(queryGraph, attachData, ExplorativeSubgraphIsomorphism.MatchStrategy.HOMOMORPHISM );
  }
}
