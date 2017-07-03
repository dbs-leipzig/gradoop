
package org.gradoop.flink.io.impl.graph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Updates an EPGM edge with the given target vertex identifier.
 *
 * @param <E> EPGM edge type
 * @param <K> Import Edge/Vertex identifier type
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("f1->targetId")
public class UpdateEdge<E extends Edge, K extends Comparable<K>>
  implements JoinFunction<Tuple2<K, E>, Tuple2<K, GradoopId>, E> {

  /**
   * Updates the target vertex identifier of the given EPGM edge.
   *
   * @param targetIdEdgePair import target id and EPGM edge
   * @param vertexIdPair     import target vertex id and EPGM vertex id
   * @return EPGM edge with updated target vertex id
   * @throws Exception
   */
  @Override
  public E join(Tuple2<K, E> targetIdEdgePair,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    targetIdEdgePair.f1.setTargetId(vertexIdPair.f1);
    return targetIdEdgePair.f1;
  }
}
