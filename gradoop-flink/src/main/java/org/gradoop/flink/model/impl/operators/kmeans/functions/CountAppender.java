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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Appends a count variable to the tuple.
 */
@FunctionAnnotation.ForwardedFields("1")
public class CountAppender implements MapFunction<Tuple2<Centroid, Point>, Tuple3<Integer, Point, Long>> {

  /**
   * Appends a counter to the tuple, responsible for counting the amount of summed up points per centroid.
   *
   * @param centroidWithAssignedPoint CentroidId with a point assigned to it
   * @return Returns the CentroidId with its assigned point, extended by a counter
   */
  @Override
  public Tuple3<Integer, Point, Long> map(Tuple2<Centroid, Point> centroidWithAssignedPoint) {
    return new Tuple3<>(centroidWithAssignedPoint.f0.getId(), centroidWithAssignedPoint.f1, 1L);
  }
}
