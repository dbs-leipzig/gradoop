
package org.gradoop.flink.model.impl.operators.neighborhood.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Puts the vertex to the first field of the tuple.
 *
 * @param <K> type of the first field of the tuple
 * @param <V> type of the second field of the tuple
 */
public class VertexToFieldZero<K, V>
  implements JoinFunction<Tuple2<K, V>, Vertex, Tuple2<Vertex, V>> {

  /**
   * Avoid object instantiation.
   */
  private Tuple2<Vertex, V> reuseTuple = new Tuple2<Vertex, V>();

  @Override
  public Tuple2<Vertex, V> join(Tuple2<K, V> tuple, Vertex vertex) throws Exception {
    reuseTuple.setFields(vertex, tuple.f1);
    return reuseTuple;
  }
}

