package org.gradoop.model.impl.operators.cam.functions;

import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;

public class EdgeDataLabeler<E extends EPGMEdge>
  extends EPGMElementDataLabeler
  implements EdgeLabeler<E> {

  @Override
  public void flatMap(E edge, Collector<EdgeLabel> collector) throws Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String edgeLabel = "[" + label(edge) + "]";

    for(GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new EdgeLabel(graphId, sourceId, targetId, edgeLabel));
    }

  }
}
