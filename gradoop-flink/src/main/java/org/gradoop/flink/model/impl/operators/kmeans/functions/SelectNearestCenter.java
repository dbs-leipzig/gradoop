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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.kmeans.util.Centroid;
import org.gradoop.flink.model.impl.operators.kmeans.util.Point;

import java.util.Collection;

/**
 * Assigns the nearest centroid to a point.
 */
public class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Centroid, Point>> {

  /**
   * Centroids to which the points are assigned
   */
  private Collection<Centroid> centroids;

  /**
   * Closest centroid for a given point
   */
  private Centroid closestCentroid;

  /**
   * Reads the centroid values from a broadcast variable into a collection.
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
  }

  /**
   * Searches the nearest centroid for a given point out of all centroids.
   *
   * @param p Given point
   * @return Returns a tuple containing the point and its nearest centroid
   */
  @Override
  public Tuple2<Centroid, Point> map(Point p) {
    double minDistance = Double.MAX_VALUE;
    // check all cluster centers
    for (Centroid centroid : centroids) {
      // compute distance
      double distance = p.euclideanDistance(centroid);
      // update nearest cluster if necessary
      if (distance < minDistance) {
        minDistance = distance;
        closestCentroid = centroid;
      }
    }
    // emit a new record with the center id and the data point.
    return new Tuple2<>(closestCentroid, p);
  }
}

