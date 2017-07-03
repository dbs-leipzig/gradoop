
package org.gradoop.flink.model.impl.operators.cloning.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Joins edges with a Tuple2 that contains the id of the original edge
 * source in its first field and the id of the new edge source vertex in its
 * second.
 * The output is an edge with updated source id.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsSecond("f1->sourceId")
public class EdgeSourceUpdateJoin<E extends Edge>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopId>, E> {
  /**
   * {@inheritDoc}
   */
  @Override
  public E join(E e, Tuple2<GradoopId, GradoopId> vertexTuple) {
    e.setSourceId(vertexTuple.f1);
    return e;
  }
}
