/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
