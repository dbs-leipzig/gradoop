package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.SubgraphIsomorphismTest;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser
  .TraverserStrategy;

public class ExplorativeIsomorphismTriplesForLoopTest extends SubgraphIsomorphismTest {

  public ExplorativeIsomorphismTriplesForLoopTest(String testName, String dataGraph,
    String queryGraph, String expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    return new ExplorativePatternMatching.Builder()
      .setQuery(queryGraph)
      .setAttachData(attachData)
      .setMatchStrategy(MatchStrategy.ISOMORPHISM)
      .setTraverserStrategy(TraverserStrategy.TRIPLES_FOR_LOOP_ITERATION)
      .build();
  }
}
