package org.gradoop.model.impl.operators.matching.simulation.dual;

import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHead;
import org.gradoop.model.impl.pojo.VertexPojo;

/**
 * Creates an {@link DualSimulation} instance that used delta iteration.
 */
public class DualSimulationDeltaTest extends DualSimulationTest {

  public DualSimulationDeltaTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching<GraphHead, VertexPojo, EdgePojo> getImplementation(
    String queryGraph, boolean attachData) {
    return new DualSimulation<>(queryGraph, attachData, false);
  }
}
