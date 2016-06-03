package org.gradoop.model.impl.operators.matching.isomorphism.explorative;

import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.operators.matching.isomorphism.SubgraphIsomorphismTest;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

public class ExplorativeSubgraphIsomorphismTest extends SubgraphIsomorphismTest {

  public ExplorativeSubgraphIsomorphismTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching<GraphHeadPojo, VertexPojo, EdgePojo> getImplementation(
    String queryGraph, boolean attachData) {
    return new ExplorativeSubgraphIsomorphism<>(queryGraph, attachData);
  }
}
