/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;
import java.util.TreeMap;

/**
 * A group reduce and reduce function collecting all edge time intervals of a vertex id to build a tree data
 * structure.
 */
public class BuildTemporalDegreeTree implements
  GroupReduceFunction<Tuple3<GradoopId, Long, Long>, Tuple2<GradoopId, TreeMap<Long, Integer>>>,
  ReduceFunction<Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long>> {

  /**
   * Definition of the TreeMap here to reduce object instantiations.
   */
  private final TreeMap<Long, Integer> degreeTreeMap;

  /**
   * Creates an instance of this reduce function class.
   */
  public BuildTemporalDegreeTree() {
    this.degreeTreeMap = new TreeMap<>();
  }

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, Long, Long>> iterable,
    Collector<Tuple2<GradoopId, TreeMap<Long, Integer>>> collector) throws Exception {

    // Clear the tree
    this.degreeTreeMap.clear();

    GradoopId vertexId = null;

    for (Tuple3<GradoopId, Long, Long> entity : iterable) {
      // since we group on the id, we just need it once
      if (vertexId == null) {
        vertexId = entity.f0;
      }
      // add time interval to tree map - increment for start timestamp, decrement for end timestamp
      degreeTreeMap.merge(entity.f1, 1, Integer::sum);
      degreeTreeMap.merge(entity.f2, -1, Integer::sum);
    }
    collector.collect(new Tuple2<>(vertexId, degreeTreeMap));
  }

  @Override
  public Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> reduce(
    Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> left,
    Tuple4<GradoopId, TreeMap<Long, Integer>, Long, Long> right) throws Exception {

    // Put all elements of the right into the left tree and return the left one as merged tree
    for (Map.Entry<Long, Integer> entry : right.f1.entrySet()) {
      left.f1.merge(entry.getKey(), entry.getValue(), Integer::sum);
    }
    return left;
  }
}
