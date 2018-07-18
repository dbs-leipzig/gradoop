/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Merges multiple fat vertices into a single {@link FatVertex}.
 *
 * [fatVertex] -> fatVertex
 *
 * Forwarded fields:
 *
 * f0: vertex id
 *
 * Read fields:
 *
 * f1: vertex query candidates
 * f2: parent ids
 * f3: counters for incoming edge candidates
 * f4: outgoing edges (edgeId, targetId) and their query candidates
 *
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f1;f2;f3;f4")
public class GroupedFatVertices implements
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

  /**
   * Merges two fat vertices.
   *
   * @param result base
   * @param diff   diff to merge
   * @return base merged with diff
   */
  private FatVertex merge(FatVertex result, FatVertex diff) {
    // update vertex candidates (CA)
    if (diff.getCandidates().removeAll(result.getCandidates())) {
      result.getCandidates().addAll(diff.getCandidates());
    }
    // update parent ids (P_IDs)
    result.getParentIds().addAll(diff.getParentIds());
    // update incoming edge counts (IN_CA)
    for (int i = 0; i < diff.getIncomingCandidateCounts().length; i++) {
      result.getIncomingCandidateCounts()[i] += diff
        .getIncomingCandidateCounts()[i];
    }
    // update outgoing edges (OUT_CA)
    result.getEdgeCandidates().putAll(diff.getEdgeCandidates());

    return result;
  }
}
