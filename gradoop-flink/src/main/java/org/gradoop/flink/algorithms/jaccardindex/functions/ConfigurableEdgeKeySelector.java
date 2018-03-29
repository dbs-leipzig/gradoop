package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Direction.OUTDEGREE;

public class ConfigurableEdgeKeySelector implements KeySelector<Edge, GradoopId> {

  private Direction direction;

  public ConfigurableEdgeKeySelector(Direction direction) {
    this.direction = direction;
  }

  @Override
  public GradoopId getKey(Edge value) throws Exception {
    return direction.equals(OUTDEGREE) ? value.getSourceId() : value.getTargetId();
  }
}
