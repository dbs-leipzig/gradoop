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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.datasetiteration.IterativeTuple.EdgeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps a group of {@code Tuple2<GradoopId, GradoopId}, with each tuple representing an outgoing
 * edge from the same source vertex, to a {@code Tuple2<GradoopId, List<EdgeType>>} containing
 * this source vertex id and a list of all outgoing edges of type {@link EdgeType}. Also stores
 * the algorithms jump probability in all edges - for reasons regarding the algorithm design and
 * simplifying the access to this value while iterating.
 */
public class SourceTargetToSourceTargetsListGroupReduce implements
  GroupReduceFunction<Tuple2<GradoopId, GradoopId>,
    Tuple2<GradoopId, List<EdgeType>>> {

  /**
   * Probability for jumping to a random vertex instead of walking to a random neighbor
   */
  private final double jumpProbability;

  /**
   * Creates an instance of SourceTargetToSourceTargetsListGroupReduce
   *
   * @param jumpProbability Probability for jumping to a random vertex instead of walking to
   *                        a random neighbor
   */
  public SourceTargetToSourceTargetsListGroupReduce(double jumpProbability) {
    this.jumpProbability = jumpProbability;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> in,
    Collector<Tuple2<GradoopId, List<EdgeType>>> out) throws Exception {
    GradoopId sourceId = new GradoopId();
    List<EdgeType> edges = new ArrayList<>();
    for (Tuple2<GradoopId, GradoopId> edgeTuple : in) {
      sourceId = edgeTuple.f0;
      edges.add(new EdgeType(0L, edgeTuple.f1, jumpProbability));
    }
    out.collect(Tuple2.of(sourceId, edges));
  }
}
