
package org.gradoop.flink.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Adds new graph ids to the initial vertex set
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ReadFieldsFirst("graphIds")
@FunctionAnnotation.ReadFieldsSecond("f1")
public class AddNewGraphsToVertex<V extends Vertex>
  implements JoinFunction<V, Tuple2<GradoopId, GradoopIdList>, V> {
  /**
   * {@inheritDoc}
   */
  @Override
  public V join(V vertex,
    Tuple2<GradoopId, GradoopIdList> vertexWithGraphIds) {
    vertex.getGraphIds().addAll(vertexWithGraphIds.f1);
    return vertex;
  }

}
