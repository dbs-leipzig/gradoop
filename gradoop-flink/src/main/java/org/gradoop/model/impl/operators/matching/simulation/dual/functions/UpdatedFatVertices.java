package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .FatVertex;

/**
 * Filters a {@link FatVertex} that has been updated during the previous
 * iteration.
 *
 * Read fields:
 *
 * f5: updated flag
 */
@FunctionAnnotation.ReadFields("f5")
public class UpdatedFatVertices implements FilterFunction<FatVertex> {

  @Override
  public boolean filter(FatVertex fatVertex) throws Exception {
    return fatVertex.isUpdated();
  }
}
