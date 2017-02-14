package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.flink.model.impl.operators.join.functions.OplusVertex;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.tuples.ResultingJoinVertex;

import java.io.Serializable;

/**
 * Created by vasistas on 01/02/17.
 */
public class VertexPJoinCondition<PV> implements
  FlatJoinFunction<Tuple2<Vertex,OptSerializableGradoopId>, Tuple2<Vertex,OptSerializableGradoopId>, ResultingJoinVertex>, Serializable {
  private final Function<Tuple2<Vertex,Vertex>,Boolean> thetaVertexF;
  private final OplusVertex combineVertices;

  public VertexPJoinCondition(Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF,
    OplusVertex combineVertices) {
    this.thetaVertexF = thetaVertexF;
    this.combineVertices = combineVertices;
  }

  @Override
  public void join(Tuple2<Vertex,OptSerializableGradoopId> first, Tuple2<Vertex, OptSerializableGradoopId> second, Collector<ResultingJoinVertex> out) throws
    Exception {
    if (first != null && second != null && first.f1.isPresent() && second.f1.isPresent()) {
      Vertex ff0 = first.getField(0), sf0 = second.getField(0);
      if (ff0==null)
        throw new RuntimeException("ff0 is null");
      else if (sf0 ==null)
        throw new RuntimeException("sf0 is null");
      if (thetaVertexF.apply(new Tuple2<>(ff0,sf0))) {
        out.collect(new ResultingJoinVertex(OptSerializableGradoopId.value(ff0.getId()),
          OptSerializableGradoopId.value(sf0.getId()),
          combineVertices.apply(new Tuple2<>(ff0,sf0))));
      }
    } else {
      if (first == null || !first.f1.isPresent()) {
        Vertex sf0 = second.getField(0);
        out.collect(
          new ResultingJoinVertex(OptSerializableGradoopId.empty(), OptSerializableGradoopId.value(sf0.getId()),
            sf0));
      }
      if (second == null || !second.f1.isPresent()){
        Vertex ff0 = first.getField(0);
        out.collect(
          new ResultingJoinVertex(OptSerializableGradoopId.value(ff0.getId()), OptSerializableGradoopId.empty(),
            ff0));
      }
    }
  }
}
