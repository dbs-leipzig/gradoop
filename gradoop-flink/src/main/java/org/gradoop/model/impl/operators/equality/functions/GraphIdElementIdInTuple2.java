package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Element@(GraphID1,..,GraphIdn) =>
 * (GraphID1,ElementId),..,(GraphIDn,ElementId)
 *
 * @param <GE> vertex or edge type
 */
public class GraphIdElementIdInTuple2<GE extends EPGMGraphElement>
  implements FlatMapFunction<GE, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    GE element, Collector<Tuple2<GradoopId, GradoopId>> collector
  ) throws Exception {

    GradoopId elementId = element.getId();

    for (GradoopId graphId : element.getGraphIds()) {
      collector.collect(new Tuple2<>(graphId, elementId));
    }

  }
}
