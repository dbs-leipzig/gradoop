package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;


import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.IdPair;

/**
 * Collects all edges contained in a {@link FatVertex}.
 */
@FunctionAnnotation.ReadFields("f4")
public class EdgeTriple implements
  FlatMapFunction<FatVertex, Tuple3<GradoopId, GradoopId, GradoopId>> {

  private final Tuple3<GradoopId, GradoopId, GradoopId> reuseTuple;

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
