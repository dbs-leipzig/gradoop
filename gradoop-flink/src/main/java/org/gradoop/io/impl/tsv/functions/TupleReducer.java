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

package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.HashMap;
import java.util.Map;

/**
 * GroupReduceFunction to reduce duplicated (original) source and target ids.
 */
public class TupleReducer implements GroupReduceFunction<Tuple6<String,
  GradoopId, String, String, GradoopId, String>, Tuple6<String,
    GradoopId, String, String, GradoopId, String>> {

  /**
   * For each Tuple: check if source or target origin id is unique.
   * Iff not set unique GradoopId for each duplicate
   *
   * @param iterable    tuple set
   * @param collector   reduce collector
   * @throws Exception
   */
  @Override
  public void reduce(
    Iterable<Tuple6<String, GradoopId, String, String, GradoopId, String>>
      iterable,
    Collector<Tuple6<String, GradoopId, String, String, GradoopId, String>>
      collector) throws Exception {

    Map<String, GradoopId> sourceDuplicates = new HashMap<>();
    Map<String, GradoopId> targetDuplicates = new HashMap<>();

    for (Tuple6<String, GradoopId, String, String, GradoopId, String>
      tuple: iterable) {

      if (!(sourceDuplicates.containsKey(tuple.f0) ||
              targetDuplicates.containsKey(tuple.f3))) {
        collector.collect(tuple);
      } else {
        if (sourceDuplicates.containsKey(tuple.f0)) {
          tuple.f1 = sourceDuplicates.get(tuple.f0);
          collector.collect(tuple);
        }
        if (targetDuplicates.containsKey(tuple.f3)) {
          tuple.f4 = targetDuplicates.get(tuple.f3);
          collector.collect(tuple);

        }
      }
      sourceDuplicates.put(tuple.f0, tuple.f1);
      targetDuplicates.put(tuple.f3, tuple.f4);
    }
  }
}
