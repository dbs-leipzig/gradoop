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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;
import java.util.TreeMap;

/**
 * Base class for calculating the degree evolution by the given degree tree.
 */
public abstract class BaseCalculateDegrees {

  /**
   * Exception message string.
   */
  private static final String TEMPORAL_VIOLATION_MSG = "Last timestamp [%d] is not smaller than the " +
    "current [%d] for vertex with id [%s]. A chronological order of timestamps is mandatory. Please " +
    "check the temporal integrity of your graph in the given time domain. The operator " +
    "TemporalGraph#updateEdgeValidity() can be used to update an edges validity to ensure its integrity.";

  /**
   * Function calculates and collects the degree evolution for a given vertex and degree tree. It uses the
   * from and to times of the vertex. If these times are not known, default timestamps can be used.
   *
   * @param vertexId the vertex id
   * @param degreeTree the degree tree of that vertex
   * @param vertexFromTime the from time of that vertex
   * @param vertexToTime the to time of that vertex
   * @param collector the collector that collects the resulting degree tuples
   */
  protected void calculateDegreeAndCollect(GradoopId vertexId, TreeMap<Long, Integer> degreeTree,
    Long vertexFromTime, Long vertexToTime, Collector<Tuple4<GradoopId, Long, Long, Integer>> collector) {

    // we store for each timestamp the current degree
    int degree = 0;

    // first degree 0 is from t_from(v) to the first occurrence of a start timestamp
    Long lastTimestamp = vertexFromTime;

    for (Map.Entry<Long, Integer> entry : degreeTree.entrySet()) {
      // check integrity
      if (lastTimestamp > entry.getKey()) {
        // This should not happen, seems that a temporal constraint is violated
        throw new IllegalArgumentException(String.format(TEMPORAL_VIOLATION_MSG, lastTimestamp,
          entry.getKey(), vertexId));
      }

      if (lastTimestamp.equals(entry.getKey())) {
        // First timestamp in tree is equal to the lower interval bound of the vertex
        degree += entry.getValue();
        continue;
      }

      // The payload is 0, means the degree does not change and the intervals can be merged
      if (entry.getValue() != 0) {
        collector.collect(new Tuple4<>(vertexId, lastTimestamp, entry.getKey(), degree));
        degree += entry.getValue();
        // remember the last timestamp since it is the first one of the next interval
        lastTimestamp = entry.getKey();
      }
    }

    // last degree is 0 from last occurence of timestamp to t_to(v)
    if (lastTimestamp < vertexToTime) {
      collector.collect(new Tuple4<>(vertexId, lastTimestamp, vertexToTime, degree));
    } else if (lastTimestamp > vertexToTime) {
      // This should not happen, seems that a temporal constraint is violated
      throw new IllegalArgumentException(String.format(TEMPORAL_VIOLATION_MSG, lastTimestamp, vertexToTime,
        vertexId));
    } // else, the ending bound of the vertex interval equals the last timestamp of the edges
  }
}
