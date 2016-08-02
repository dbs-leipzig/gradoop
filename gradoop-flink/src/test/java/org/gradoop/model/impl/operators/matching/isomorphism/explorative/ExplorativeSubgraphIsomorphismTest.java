package org.gradoop.model.impl.operators.matching.isomorphism.explorative;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.isomorphism
  .explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.common.model.impl.operators.matching.isomorphism.SubgraphIsomorphismTest;

public class ExplorativeSubgraphIsomorphismTest extends SubgraphIsomorphismTest {

  public ExplorativeSubgraphIsomorphismTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph,
    boolean attachData) {
    return new ExplorativeSubgraphIsomorphism(queryGraph, attachData);
  }
}
