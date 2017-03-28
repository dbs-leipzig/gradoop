package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.SubgraphHomomorphismTest;

/**
 * Runs test for vertex and edge homomorphisms.
 */
public class CypherPatternMatchingHomomorphismTest extends SubgraphHomomorphismTest {

  public CypherPatternMatchingHomomorphismTest(String testName, String dataGraph, String queryGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    int n = 42; // just used for testing
    return new CypherPatternMatching("MATCH " + queryGraph, attachData,
      MatchStrategy.HOMOMORPHISM, MatchStrategy.HOMOMORPHISM,
      new GraphStatistics(n, n, n, n));
  }
}
