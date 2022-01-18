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
package org.gradoop.flink.model.impl.operators.kmeans.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Sums up the points that are assigned to the same centroid. Counts how many were points summed up for
 * every centroid.
 */
@FunctionAnnotation.ForwardedFields("0")
public class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

  /**
   * Tuples contain the centroidId, the assigned point and the counter of the centroid.
   * Reduces both tuples by adding up the points and increasing the counter.
   *
   * @param val1 First point assigned to the centroid, together with its counter
   * @param val2 Second point assigned to the centroid, together with its counter
   * @return Returns the centroidId, the summed up points and the incremented counter
   */
  @Override
  public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1,
    Tuple3<Integer, Point, Long> val2) {
    return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
  }
}
