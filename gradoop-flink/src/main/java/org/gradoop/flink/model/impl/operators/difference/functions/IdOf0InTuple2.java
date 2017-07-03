
package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the identifier of first element in a tuple 2.
 *
 * @param <GD> graph data type
 * @param <C>  type of second element in tuple
 */
@FunctionAnnotation.ForwardedFields("f0.id->*")
public class IdOf0InTuple2<GD extends GraphHead, C>
  implements KeySelector<Tuple2<GD, C>, GradoopId> {

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(Tuple2<GD, C> pair) throws Exception {
    return pair.f0.getId();
  }
}
