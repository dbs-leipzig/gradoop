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

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.TreeSet;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A map partition function that merges/aggregates all Tuples (vId, degreeTree) on a partition into a
 * new Tuple (GradoopId, degreeTree, Integer) that represents a new "super" vertex which is the aggregated
 * combination of all vertices on that partition. The Integer represents the count of
 * degreeTrees that were used to create this new one.
 */
public class MapPartitionCombineDegreeTrees
        implements MapPartitionFunction<Tuple2<GradoopId, TreeMap<Long, Integer>>,
                                        Tuple3<GradoopId, TreeMap<Long, Integer>, Integer>> {

  /**
   * The aggregate type to use (min,max,avg).
   */
  private final AggregationType aggregateType;

   /**
   * Creates an instance of this map partition function.
   *
   * @param aggregateType the aggregate type to use (min,max,avg).
   */
  public MapPartitionCombineDegreeTrees(AggregationType aggregateType) {
    this.aggregateType = aggregateType;
  }

  @Override
  public void mapPartition(Iterable<Tuple2<GradoopId, TreeMap<Long, Integer>>> iterable,
                           Collector<Tuple3<GradoopId, TreeMap<Long, Integer>, Integer>> collector)
          throws Exception {

    // init necessary maps and set
    HashMap<GradoopId, TreeMap<Long, Integer>> degreeTrees = new HashMap<>();
    HashMap<GradoopId, Integer> vertexDegrees = new HashMap<>();
    TreeMap<Long, Integer> values = new TreeMap<>();
    SortedSet<Long> timePoints = new TreeSet<>();

    // convert the iterables to a hashmap and remember all possible timestamps
    for (Tuple2<GradoopId, TreeMap<Long, Integer>> tuple : iterable) {
      degreeTrees.put(tuple.f0, tuple.f1);
      timePoints.addAll(tuple.f1.keySet());
    }

    int numberOfVertices = degreeTrees.size();

    //check if partition is empty, if yes, collect nothing and return
    if (numberOfVertices == 0) {
      return;
    }

    int degreeValue = 0;

    // Add default times
    timePoints.add(Long.MIN_VALUE);

    //create new GradoopId for the new "super-vertex"
    GradoopId key = GradoopId.get();

    for (Long timePoint : timePoints) {
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

      switch (aggregateType) {
      case MIN:
        degreeValue = vertexDegrees.values().stream().reduce(Math::min).orElse(0);
        break;
      case MAX:
        degreeValue = vertexDegrees.values().stream().reduce(Math::max).orElse(0);
        break;
      case AVG:
        degreeValue = vertexDegrees.values().stream().reduce(Math::addExact).orElse(0);
        break;
      default:
        throw new IllegalArgumentException("Aggregate type not specified.");
      }
      values.put(timePoint, degreeValue);
    }
    //collect the results
    collector.collect(new Tuple3<>(key, values, numberOfVertices));
  }
}
