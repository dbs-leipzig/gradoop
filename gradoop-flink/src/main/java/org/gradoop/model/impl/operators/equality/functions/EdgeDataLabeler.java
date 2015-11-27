package org.gradoop.model.impl.operators.equality.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;

/**
 * Created by peet on 23.11.15.
 */
public class EdgeDataLabeler<E extends EPGMEdge>
  extends ElementBaseLabeler
  implements MapFunction<E, EdgeDataLabel>, FlatMapFunction<E, EdgeDataLabel> {

  @Override
  public EdgeDataLabel map(E edge) throws Exception {
    return initDataLabel(edge);
  }

  @Override
  public void flatMap(E edge, Collector<EdgeDataLabel> collector) throws
    Exception {
    EdgeDataLabel dataLabel = initDataLabel(edge);

    for(GradoopId graphId : edge.getGraphIds()) {
      dataLabel.setGraphId(graphId);
      collector.collect(dataLabel);
    }
  }

  private EdgeDataLabel initDataLabel(E edge) {
    String canonicalLabel = edge.getLabel() + label(edge.getProperties()) ;

    return new EdgeDataLabel(edge.getSourceId(), edge.getTargetId(),
      canonicalLabel);
  }
}
