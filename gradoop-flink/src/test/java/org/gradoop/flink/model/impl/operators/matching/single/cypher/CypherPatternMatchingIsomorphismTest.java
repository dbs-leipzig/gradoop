package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.SubgraphIsomorphismTest;

/**
 * Runs test for vertex and edge isomorphism.
 */
public class CypherPatternMatchingIsomorphismTest extends SubgraphIsomorphismTest {

  public CypherPatternMatchingIsomorphismTest(String testName, String dataGraph, String queryGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    int n = 42; // just used for testing
    return new CypherPatternMatching("MATCH " + queryGraph, attachData,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
      new GraphStatistics(n, n, n, n));
  }
}
