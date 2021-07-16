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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

/**
 * Creates a key assigned to each vertex with its spatial properties. Used for joining the modified
 * vertices with the original ones.
 */


public class PointToKey implements MapFunction<Tuple2<Centroid, Point>, Tuple2<Centroid, String>> {

  /**
   * Transforms a centroid and the point assigned to it, to a centroid and a unique key, representing the
   * point
   *
   * @param centroidPointTuple2 Centroid and the point assigned to it.
   * @return Replaces the points with its unique key and returns the tuple.
   */
  @Override
  public Tuple2<Centroid, String> map(Tuple2<Centroid, Point> centroidPointTuple2) {
    return new Tuple2<>(centroidPointTuple2.f0,
      centroidPointTuple2.f1.getLat().toString() + ";" + centroidPointTuple2.f1.getLon());
  }
}
