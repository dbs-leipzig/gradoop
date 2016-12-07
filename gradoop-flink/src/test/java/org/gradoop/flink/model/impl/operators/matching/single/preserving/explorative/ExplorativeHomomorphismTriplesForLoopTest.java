package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.SubgraphHomomorphismTest;

public class ExplorativeHomomorphismTriplesForLoopTest extends SubgraphHomomorphismTest {

  public ExplorativeHomomorphismTriplesForLoopTest(String testName,
    String dataGraph, String queryGraph, String expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    return new ExplorativePatternMatching.Builder()
      .setQuery(queryGraph)
      .setAttachData(attachData)
      .setMatchStrategy(MatchStrategy.HOMOMORPHISM)
      .setTraverserStrategy(TraverserStrategy.TRIPLES_FOR_LOOP_ITERATION)
      .build();
  }
}
