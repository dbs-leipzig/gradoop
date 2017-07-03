package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

/**
 * represents an edge by an id string
 */
public class EdgeToIdString extends ElementToDataString implements
  EdgeToString<Edge> {

  @Override
  public void flatMap(Edge edge, Collector<EdgeString> collector)
      throws Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String edgeLabel = "[" + edge.getId() + "]";

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new EdgeString(graphId, sourceId, targetId, edgeLabel));
    }

  }
}
