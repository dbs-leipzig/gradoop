package org.gradoop.model.impl.operators.matching.simulation.dual;

import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

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
  public PatternMatching<GraphHeadPojo, VertexPojo, EdgePojo> getImplementation(
    String queryGraph, boolean attachData) {
    return new DualSimulation<>(queryGraph, attachData, true);
  }
}
