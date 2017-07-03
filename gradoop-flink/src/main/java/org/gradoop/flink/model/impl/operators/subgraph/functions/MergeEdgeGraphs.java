
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Reduces groups of tuples 4 consisting of 4 gradoop ids
 * into one tuple per group, containing the first three gradoop ids of the
 * first element in this group and a gradoop id set, containing all gradoop
 * ids in the fourth slot.
 */

@FunctionAnnotation.ForwardedFieldsFirst("f0->f0;f2->f2;f3->f3")
@FunctionAnnotation.ForwardedFieldsSecond("f1->f1")
public class MergeEdgeGraphs implements
  GroupReduceFunction<
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>,
    Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdList>> {

  @Override
  public void reduce(
    Iterable<Tuple4<GradoopId, GradoopId, GradoopId, GradoopId>> iterable,
    Collector<
      Tuple4<GradoopId, GradoopId, GradoopId, GradoopIdList>> collector) {

    GradoopIdList set = new GradoopIdList();

    boolean empty = true;
    GradoopId f0 = null;
    GradoopId f1 = null;
    GradoopId f2 = null;

    for (Tuple4<GradoopId, GradoopId, GradoopId, GradoopId> tuple : iterable) {
      set.add(tuple.f3);
      empty = false;
      f0 = tuple.f0;
      f1 = tuple.f1;
      f2 = tuple.f2;
    }

    if (!empty) {
      collector.collect(new Tuple4<>(f0, f1, f2, set));
    }
  }
}
