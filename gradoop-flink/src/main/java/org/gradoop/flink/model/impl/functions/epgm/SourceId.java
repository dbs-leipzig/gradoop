
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Used to select the source vertex id of an edge.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->*")
public class SourceId<E extends Edge>
  implements KeySelector<E, GradoopId>, MapFunction<E, GradoopId> {

  @Override
  public GradoopId getKey(E edge) throws Exception {
    return edge.getSourceId();
  }

  @Override
  public GradoopId map(E edge) throws Exception {
    return edge.getSourceId();
  }
}
