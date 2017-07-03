
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a new {@link GradoopId} for the input element in a Tuple1 and
 * returns both.
 *
 * @param <T>
 */
@FunctionAnnotation.ForwardedFields("f0")
public class PairTupleWithNewId<T>
  implements MapFunction<Tuple1<T>, Tuple2<T, GradoopId>> {

  /**
   * Reduce object instantiations
   */
  private final Tuple2<T, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public Tuple2<T, GradoopId> map(Tuple1<T> input) {
    reuseTuple.f0 = input.f0;
    reuseTuple.f1 = GradoopId.get();
    return reuseTuple;
  }
}
