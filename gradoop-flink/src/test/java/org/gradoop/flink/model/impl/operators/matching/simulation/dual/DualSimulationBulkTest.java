package org.gradoop.flink.model.impl.operators.matching.simulation.dual;

import org.gradoop.flink.model.impl.operators.matching.PatternMatching;

/**
 * Creates an {@link DualSimulation} instance that used bulk iteration.
 */
public class DualSimulationBulkTest extends DualSimulationTest {


  public DualSimulationBulkTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph,
    boolean attachData) {
    return new DualSimulation(queryGraph, attachData, true);
  }
}
