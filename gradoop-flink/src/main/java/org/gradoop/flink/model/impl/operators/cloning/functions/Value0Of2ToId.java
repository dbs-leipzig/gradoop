
package org.gradoop.flink.model.impl.operators.cloning.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Maps the second value of a Tuple2 to its gradoop id.
 *
 * @param <T> type of first field
 * @param <EL> element type of second field
 */
@FunctionAnnotation.ForwardedFields("f0.id->f0;f1")
public class Value0Of2ToId<EL extends Element, T>
  implements
  MapFunction<Tuple2<EL, T>, Tuple2<GradoopId, T>> {

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<GradoopId, T> reuseTuple = new Tuple2<>();

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, T> map(Tuple2<EL, T> tuple2) {
    reuseTuple.setFields(tuple2.f0.getId(), tuple2.f1);
    return reuseTuple;
  }
}
