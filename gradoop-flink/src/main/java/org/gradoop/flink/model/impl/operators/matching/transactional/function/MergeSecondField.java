
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.util.Iterator;

/**
 * Merges the second field of Tuple2, containing GradoopIds, to a GradoopIdSet.
 * @param <T> any type
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1")
public class MergeSecondField<T>
  implements GroupReduceFunction<Tuple2<T, GradoopId>, Tuple2<T, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<T, GradoopId>> iterable,
    Collector<Tuple2<T, GradoopIdList>> collector) throws Exception {
    Iterator<Tuple2<T, GradoopId>> it = iterable.iterator();
    Tuple2<T, GradoopId> firstTuple = it.next();
    T firstField = firstTuple.f0;
    GradoopIdList secondField = GradoopIdList.fromExisting(firstTuple.f1);
    while (it.hasNext()) {
      GradoopId id = it.next().f1;
      secondField.add(id);
    }
    collector.collect(new Tuple2<>(firstField, secondField));
  }
}
