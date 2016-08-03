package org.gradoop.flink.model.impl.operators.matching.simulation.dual;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;

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
  public PatternMatching getImplementation(String queryGraph,
    boolean attachData) {
    return new DualSimulation(queryGraph, attachData, false);
  }
}
