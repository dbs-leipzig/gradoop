
package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * (edge) -> (sourceId, label)
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;label->f1")
public class ToSourceIdWithLabel<E extends Edge> implements MapFunction<E, IdWithLabel> {
  /**
   * Reuse tuple
   */
  private final IdWithLabel reuseTuple = new IdWithLabel();

  @Override
  public IdWithLabel map(E edge) throws Exception {
    reuseTuple.setId(edge.getSourceId());
    reuseTuple.setLabel(edge.getLabel());
    return reuseTuple;
  }
}
