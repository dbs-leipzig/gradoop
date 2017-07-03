
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Returns one Tuple2<GradoopId, T> per id contained in the first field.
 * @param <T> any type
 */
@FunctionAnnotation.ForwardedFields("f1")
public class ExpandFirstField<T>
  implements FlatMapFunction<Tuple2<GradoopIdList, T>, Tuple2<GradoopId, T>> {

  /**
   * Reduce instantiation
   */
  private Tuple2<GradoopId, T> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(Tuple2<GradoopIdList, T> tuple2, Collector<Tuple2<GradoopId, T>> collector)
    throws Exception {

    reuseTuple.f1 = tuple2.f1;
    for (GradoopId id : tuple2.f0) {
      reuseTuple.f0 = id;
      collector.collect(reuseTuple);
    }
  }
}
