
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Filters a {@link FatVertex} if it has query candidates.
 *
 * Read fields:
 *
 * f1: vertex query candidates
 *
 */
@FunctionAnnotation.ReadFields("f1")
public class ValidFatVertices implements FilterFunction<FatVertex> {

  @Override
  public boolean filter(FatVertex fatVertex) throws Exception {
    return fatVertex.getCandidates().size() > 0;
  }
}
