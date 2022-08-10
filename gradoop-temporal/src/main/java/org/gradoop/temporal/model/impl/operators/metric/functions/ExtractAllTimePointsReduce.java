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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Reduce function to extract all timestamps where the degree of a vertex changes.
 */
public class ExtractAllTimePointsReduce implements GroupReduceFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>, Tuple1<Long>> {

  /**
   * Creates an instance of the group reduce function.
   */
  public ExtractAllTimePointsReduce() {
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, TreeMap<Long, Integer>>> iterable,
                     Collector<Tuple1<Long>> collector) throws Exception {
    SortedSet<Long> timePoints = new TreeSet<>();

    for (Tuple2<GradoopId, TreeMap<Long, Integer>> tuple : iterable) {
      timePoints.addAll(tuple.f1.keySet());
    }

    for (Long timePoint : timePoints) {
      collector.collect(new Tuple1<>(timePoint));
    }

  }
}
