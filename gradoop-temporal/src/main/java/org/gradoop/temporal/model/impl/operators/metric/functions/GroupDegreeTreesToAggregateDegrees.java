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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A group reduce function that merges all Tuples (vId, degreeTree) to a dataset of tuples (time, aggDegree)
 * that represents the aggregated degree value for the whole graph at the given time.
 */
public class GroupDegreeTreesToAggregateDegrees
  implements GroupReduceFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>, Tuple2<Long, Integer>> {

  /**
   * The aggregate type to use (min,max,avg).
   */
  private final AggregateType aggregateType;

  /**
   * Creates an instance of this group reduce function.
   *
   * @param aggregateType the aggregate type to use (min,max,avg).
   */
  public GroupDegreeTreesToAggregateDegrees(AggregateType aggregateType) {
    this.aggregateType = aggregateType;
  }

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, TreeMap<Long, Integer>>> iterable,
    Collector<Tuple2<Long, Integer>> collector) throws Exception {

    // init necessary maps and set
    HashMap<GradoopId, TreeMap<Long, Integer>> degreeTrees = new HashMap<>();
    HashMap<GradoopId, Integer> vertexDegrees = new HashMap<>();
    SortedSet<Long> timePoints = new TreeSet<>();

    // convert the iterables to a hashmap and remember all possible timestamps
    for (Tuple2<GradoopId, TreeMap<Long, Integer>> tuple : iterable) {
      degreeTrees.put(tuple.f0, tuple.f1);
      timePoints.addAll(tuple.f1.keySet());
    }

    int numberOfVertices = degreeTrees.size();

    // Add default times
    timePoints.add(Long.MIN_VALUE);

    for (Long timePoint : timePoints) {
      // skip last default time
      if (Long.MAX_VALUE == timePoint) {
        continue;
      }
      // Iterate over all vertices
      for (Map.Entry<GradoopId, TreeMap<Long, Integer>> entry : degreeTrees.entrySet()) {
        // Make sure the vertex is registered in the current vertexDegrees capture
        if (!vertexDegrees.containsKey(entry.getKey())) {
          vertexDegrees.put(entry.getKey(), 0);
        }

        // Check if timestamp is in tree, if not, take the lower key
        if (entry.getValue().containsKey(timePoint)) {
          vertexDegrees.put(entry.getKey(), entry.getValue().get(timePoint));
        } else {
          Long lowerKey = entry.getValue().lowerKey(timePoint);
          if (lowerKey != null) {
            vertexDegrees.put(entry.getKey(), entry.getValue().get(lowerKey));
          }
        }
      }

      // Here, every tree with this time point is iterated. Now we need to aggregate for the current time.
      Optional<Integer> opt;
      switch (aggregateType) {
      case MIN:
        opt = vertexDegrees.values().stream().reduce(Math::min);
        opt.ifPresent(integer -> collector.collect(new Tuple2<>(timePoint, integer)));
        break;
      case MAX:
        opt = vertexDegrees.values().stream().reduce(Math::max);
        opt.ifPresent(integer -> collector.collect(new Tuple2<>(timePoint, integer)));
        break;
      case AVG:
        opt = vertexDegrees.values().stream().reduce(Math::addExact);
        opt.ifPresent(integer -> collector.collect(
          new Tuple2<>(timePoint, (int) Math.ceil( (double) integer / (double) numberOfVertices))));
        break;
      default:
        throw new IllegalArgumentException("Aggregate type not specified.");
      }
    }
  }
}
