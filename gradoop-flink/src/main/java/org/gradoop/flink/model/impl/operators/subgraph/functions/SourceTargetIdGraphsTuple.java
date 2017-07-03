
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * For each edge, collect two tuple 2 containing its source or target id in the
 * first field and all the graphs this edge is contained in in its second field.
 *
 * @param <E> epgm edge type
 */

@FunctionAnnotation.ReadFields("sourceId;targetId")
@FunctionAnnotation.ForwardedFields("graphIds->f1")
public class SourceTargetIdGraphsTuple<E extends Edge>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public void flatMap(
    E e,
    Collector<Tuple2<GradoopId, GradoopIdList>> collector) throws
    Exception {

    collector.collect(new Tuple2<>(e.getSourceId(), e.getGraphIds()));
    collector.collect(new Tuple2<>(e.getTargetId(), e.getGraphIds()));
  }
}
