package org.gradoop.model.impl.operators.matching.simulation.dual;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

/**
 * Creates an {@link DualSimulation} instance that used delta iteration.
 */
public class DualSimulationDeltaTest extends DualSimulationTest {

  @Override
  protected DualSimulation<GraphHeadPojo, VertexPojo, EdgePojo> getOperator(
    String query) {
    return new DualSimulation<>(query, true, false);
  }
}
