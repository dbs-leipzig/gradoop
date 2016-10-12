package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.SubgraphIsomorphismTest;


import org.gradoop.flink.model.impl.operators.matching.preserving.explorative
  .ExplorativePatternMatching;

public class ExplorativePatternMatchingTest extends
  SubgraphIsomorphismTest {

  public ExplorativePatternMatchingTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph,
    boolean attachData) {
    return new ExplorativePatternMatching(queryGraph, attachData);
  }
}
