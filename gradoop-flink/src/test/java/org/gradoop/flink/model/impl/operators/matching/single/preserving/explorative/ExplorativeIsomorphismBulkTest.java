package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.SubgraphIsomorphismTest;

public class ExplorativeIsomorphismBulkTest extends SubgraphIsomorphismTest {

  public ExplorativeIsomorphismBulkTest(String testName, String dataGraph,
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
      .setIterationStrategy(IterationStrategy.BULK_ITERATION)
      .build();
  }
}
