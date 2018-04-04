/**
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

package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.DEFAULT_GROUP_SIZE;

/**
 * COPIED WITH SOME MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Emits the two-path for all neighbor pairs in this group.
 *
 * The first 64 vertices are emitted pairwise. Following vertices are only paired with vertices
 * from this initial group.
 *
 * @see GenerateGroupSpans
 */
public class GenerateGroupPairs implements
  GroupReduceFunction<Tuple4<IntValue, GradoopId, GradoopId, IntValue>, Tuple3<GradoopId,
    GradoopId, IntValue>> {

  /**
   * type of neighborhood
   */
  private NeighborhoodType neighborhoodType;

  /**
   * denominator
   */
  private Denominator denominator;

  /**
   * triggers the first loop
   */
  private boolean initialized = false;

  /**
   * visited edges
   */
  private List<Tuple3<GradoopId, GradoopId, IntValue>> visited;

  /**
   * Creates a new group reduce function
   * @param neighborhoodType neighborhood type
   * @param denominator denominator
   */
  public GenerateGroupPairs(NeighborhoodType neighborhoodType, Denominator
    denominator) {
    this.neighborhoodType = neighborhoodType;
    this.denominator = denominator;
    this.visited = new ArrayList<>(DEFAULT_GROUP_SIZE);
  }

  @Override
  public void reduce(Iterable<Tuple4<IntValue, GradoopId, GradoopId, IntValue>> values,
    Collector<Tuple3<GradoopId, GradoopId, IntValue>> out) throws Exception {
    int visitedCount = 0;
    for (Tuple4<IntValue, GradoopId, GradoopId, IntValue> edge : values) {
      for (int i = 0; i < visitedCount; i++) {
        Tuple3<GradoopId, GradoopId, IntValue> prior = visited.get(i);

        prior.f1 = neighborhoodType.equals(NeighborhoodType.OUT) ? edge.f1 : edge.f2;

        int oldValue = prior.f2.getValue();

        long degreeSum = getDegreeCombination(oldValue, edge.f3.getValue());
        if (degreeSum > Integer.MAX_VALUE) {
          throw new RuntimeException("Degree sum overflows IntValue");
        }
        prior.f2.setValue((int) degreeSum);

        // v, w, d(v) + d(w)
        out.collect(prior);

        prior.f2.setValue(oldValue);
      }

      if (visitedCount < DEFAULT_GROUP_SIZE) {
        if (!initialized) {
          initialized = true;

          for (int i = 0; i < DEFAULT_GROUP_SIZE; i++) {
            Tuple3<GradoopId, GradoopId, IntValue> tuple = new Tuple3<>();

            tuple.f0 = neighborhoodType
              .equals(NeighborhoodType.OUT) ? edge.f1.copy() : edge.f2.copy();
            tuple.f2 = edge.f3.copy();

            visited.add(tuple);
          }
        } else {
          Tuple3<GradoopId, GradoopId, IntValue> copy = visited.get(visitedCount);

          edge.f2.copyTo(copy.f0);
          edge.f3.copyTo(copy.f2);
        }

        visitedCount += 1;
      }
    }
  }

  /**
   * Returns the combination of the given vertex degrees in dependency of the chosen
   * {@link Denominator}
   * @param first degree of first vertex
   * @param second degree of second vertex
   * @return sum or maximum of given values
   */
  private long getDegreeCombination(int first, int second) {
    if (denominator.equals(Denominator.UNION)) {
      return first + second;
    } else {
      return Math.max(first, second);
    }
  }
}
