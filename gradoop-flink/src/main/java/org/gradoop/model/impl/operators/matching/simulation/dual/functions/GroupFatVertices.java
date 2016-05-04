package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples
  .FatVertex;

/**
 * Produces a single {@link FatVertex} from a group of fat vertices.
 */
public class GroupFatVertices implements
  GroupReduceFunction<FatVertex, FatVertex> {

  @Override
  public void reduce(Iterable<FatVertex> vertices,
    Collector<FatVertex> collector) throws Exception {

    boolean first = true;
    FatVertex result = null;

    for (FatVertex vertex : vertices) {
      if (first) {
        result = vertex;
        first = false;
      } else {
        result = merge(result, vertex);
      }
    }
    collector.collect(result);
  }

  private FatVertex merge(FatVertex result, FatVertex diff) {
    // update CA
    if (diff.getCandidates().removeAll(result.getCandidates())) {
      result.getCandidates().addAll(diff.getCandidates());
    }
    // update P_IDs
    result.getParentIds().addAll(diff.getParentIds());
    // update IN_CA
    for (int i = 0; i < diff.getIncomingCandidateCounts().length; i++) {
      result.getIncomingCandidateCounts()[i] += diff
        .getIncomingCandidateCounts()[i];
    }
    // update OUT_CA
    result.getEdgeCandidates().putAll(diff.getEdgeCandidates());

    return result;
  }
}
