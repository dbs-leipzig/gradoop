package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

/**
 * Given a vertex and an edge, coming that
 *
 * Created by Giacomo Bergami on 15/02/17.
 */
public class JoinFunctionWithVertexAndGradoopIdFromEdge implements
  JoinFunction<Vertex, Edge, Tuple2<Vertex,OptSerializableGradoopId>> {
  @Override
  public Tuple2<Vertex,OptSerializableGradoopId> join(Vertex first, Edge second) throws
    Exception {
    return new Tuple2<>(first, second == null ? OptSerializableGradoopId.empty() :
      OptSerializableGradoopId.value(second.getId()));
  }
}
