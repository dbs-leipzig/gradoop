
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Filters a {@link FatVertex} that has been updated.
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
