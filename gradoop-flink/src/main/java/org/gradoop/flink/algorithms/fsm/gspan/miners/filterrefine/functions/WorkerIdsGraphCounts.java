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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * (workerId,graphCount),.. => {workerId=graphCount,..}
 */
public class WorkerIdsGraphCounts implements GroupReduceFunction
  <Tuple2<Integer, Integer>, Map<Integer, Integer>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, Integer>> iterable,
    Collector<Map<Integer, Integer>> collector) throws Exception {

    Map<Integer, Integer> workerIdGraphCount = Maps.newHashMap();

    for (Tuple2<Integer, Integer> pair : iterable) {
      if (pair.f1 > 0) {
        workerIdGraphCount.put(pair.f0, pair.f1);
      }
    }

    collector.collect(workerIdGraphCount);
  }
}
