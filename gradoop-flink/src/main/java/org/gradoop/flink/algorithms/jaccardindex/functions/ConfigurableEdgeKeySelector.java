package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUTDEGREE;

public class ConfigurableEdgeKeySelector implements KeySelector<Edge, GradoopId> {

  private JaccardIndex.NeighborhoodType neighborhoodType;

  public ConfigurableEdgeKeySelector(NeighborhoodType neighborhoodType) {
    this.neighborhoodType = neighborhoodType;
  }

  @Override
  public GradoopId getKey(Edge value) throws Exception {
    return neighborhoodType.equals(OUTDEGREE) ? value.getSourceId() : value.getTargetId();
  }
}
