package org.gradoop.model.impl.operators.collection.binary.equality.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

public class GraphIdElementIdInTuple2<E extends EPGMGraphElement>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    E element, Collector<Tuple2<GradoopId, GradoopId>> collector
  ) throws Exception {

    GradoopId elementId = element.getId();

    for(GradoopId graphId : element.getGraphIds()) {
      collector.collect(new Tuple2<>(elementId, graphId));
    }

  }
}
