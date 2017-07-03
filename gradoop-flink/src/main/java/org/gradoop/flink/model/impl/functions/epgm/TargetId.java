
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Used to select the target vertex id of an edge.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("targetId->*")
public class TargetId<E extends Edge>
  implements KeySelector<E, GradoopId>, MapFunction<E, GradoopId> {

  @Override
  public GradoopId getKey(E edge) throws Exception {
    return edge.getTargetId();
  }

  @Override
  public GradoopId map(E edge) throws Exception {
    return edge.getTargetId();
  }
}
