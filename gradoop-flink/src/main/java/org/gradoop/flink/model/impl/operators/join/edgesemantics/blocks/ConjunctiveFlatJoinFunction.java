package org.gradoop.flink.model.impl.operators.join.edgesemantics.blocks;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.functions.OplusEdges;
import org.gradoop.flink.model.impl.operators.join.tuples.CombiningEdgeTuples;

/**
 * Created by Giacomo Bergami on 01/02/17.
 */
public class ConjunctiveFlatJoinFunction implements
  FlatJoinFunction<CombiningEdgeTuples, CombiningEdgeTuples, Edge> {

  private final Function<Tuple2<CombiningEdgeTuples,CombiningEdgeTuples>,Boolean> finalThetaEdge;
  private final OplusEdges combineEdges;

  public ConjunctiveFlatJoinFunction(
    Function<Tuple2<CombiningEdgeTuples, CombiningEdgeTuples>, Boolean> finalThetaEdge,
    OplusEdges combineEdges) {
    this.finalThetaEdge = finalThetaEdge;
    this.combineEdges = combineEdges;
  }

  @Override
  public void join(CombiningEdgeTuples first, CombiningEdgeTuples second,
    Collector<Edge> out) throws Exception {
    if (first.f0.getId().equals(second.f0.getId()) && first.f2.getId().equals(second.f2.getId())) {
      if (finalThetaEdge.apply(new Tuple2<>(first,second))) {
        Edge prepared = combineEdges.apply(new Tuple2<>(first.f1,second.f1));
        prepared.setSourceId(first.f0.getId());
        prepared.setTargetId(first.f2.getId());
        out.collect(prepared);
      }
    }
  }
}
