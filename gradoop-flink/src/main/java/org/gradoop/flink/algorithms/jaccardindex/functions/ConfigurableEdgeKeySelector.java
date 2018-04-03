package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUT;

/**
 * Selects the appropriate Key- or SourceId from an edge depending on the given
 * {@link JaccardIndex.NeighborhoodType}
 */
public class ConfigurableEdgeKeySelector implements KeySelector<Edge, GradoopId> {

  private final JaccardIndex.NeighborhoodType neighborhoodType;

  public ConfigurableEdgeKeySelector(NeighborhoodType neighborhoodType) {
    this.neighborhoodType = neighborhoodType;
  }

  @Override
  public GradoopId getKey(Edge value) throws Exception {
    return neighborhoodType.equals(OUT) ? value.getSourceId() : value.getTargetId();
  }
}
