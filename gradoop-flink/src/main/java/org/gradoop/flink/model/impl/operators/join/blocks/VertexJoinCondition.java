package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
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
public class VertexJoinCondition extends
  RichFlatJoinFunction<Vertex, Vertex, ResultingJoinVertex> implements Serializable {
  private final Function<Tuple2<Vertex,Vertex>,Boolean> thetaVertexF;
  private final OplusVertex combineVertices;

  public VertexJoinCondition(Function<Tuple2<Vertex, Vertex>, Boolean> thetaVertexF,
    OplusVertex combineVertices) {
    this.thetaVertexF = thetaVertexF;
    this.combineVertices = combineVertices;
  }

  @Override
  public void join(Vertex first, Vertex second, Collector<ResultingJoinVertex> out) throws
    Exception {
    if (first != null && second != null) {
      if (thetaVertexF.apply(new Tuple2<>(first,second))) {
        out.collect(new ResultingJoinVertex(OptSerializableGradoopId.value(first.getId()),
          OptSerializableGradoopId.value(second.getId()),
          combineVertices.apply(new Tuple2<>(first,second))));
      }
    } else if (first == null) {
      out.collect(
        new ResultingJoinVertex(OptSerializableGradoopId.empty(), OptSerializableGradoopId.value(second.getId()),
          second));
    } else {
      out.collect(
        new ResultingJoinVertex(OptSerializableGradoopId.value(first.getId()), OptSerializableGradoopId.empty(),
          first));
    }
  }
}
