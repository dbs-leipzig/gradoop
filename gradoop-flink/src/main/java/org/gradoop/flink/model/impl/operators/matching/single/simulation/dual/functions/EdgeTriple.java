
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;


import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.IdPair;

/**
 * Extracts all edges contained ina a {@link FatVertex}.
 *
 * fatVertex -> [(edgeId,sourceId,targetId)]*
 */
@FunctionAnnotation.ReadFields("f4")
public class EdgeTriple implements
  FlatMapFunction<FatVertex, Tuple3<GradoopId, GradoopId, GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple3<GradoopId, GradoopId, GradoopId> reuseTuple;

  /**
   * Constructor
   */
  public EdgeTriple() {
    reuseTuple = new Tuple3<>();
  }

  @Override
  public void flatMap(FatVertex fatVertex,
    Collector<Tuple3<GradoopId, GradoopId, GradoopId>> collector) throws
    Exception {
    reuseTuple.f1 = fatVertex.getVertexId();
    for (IdPair idPair : fatVertex.getEdgeCandidates().keySet()) {
      reuseTuple.f0 = idPair.f0;
      reuseTuple.f2 = idPair.f1;
      collector.collect(reuseTuple);
    }
  }
}
