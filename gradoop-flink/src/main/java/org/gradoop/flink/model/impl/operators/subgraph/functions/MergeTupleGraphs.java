
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Reduces groups of tuples 2 containin two gradoop ids into one tuple
 * per group, containing the first gradoop id and a gradoop id set,
 * containing all the second gradoop ids.
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class MergeTupleGraphs implements
  GroupReduceFunction<
    Tuple2<GradoopId, GradoopId>,
    Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, GradoopIdList>> collector) throws Exception {
    GradoopIdList set = new GradoopIdList();
    boolean empty = true;
    GradoopId first = null;
    for (Tuple2<GradoopId, GradoopId> tuple : iterable) {
      set.add(tuple.f1);
      empty = false;
      first = tuple.f0;
    }
    if (!empty) {
      collector.collect(new Tuple2<>(first, set));
    }
  }
}
