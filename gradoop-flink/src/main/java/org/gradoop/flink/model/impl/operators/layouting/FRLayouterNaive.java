/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.functions.FRRepulsionFunction;

/**
 * Performs a naive version of the RF-Algorithm by using the cartesian product between vertices
 * to compute repulsive-forces.
 * NOT INTENDED FOR PRACTICAL USE. Intended for performance-comparisons
 */
public class FRLayouterNaive extends FRLayouter {
  /** Create new FRLayouterNaive
   *
   * @param k Parameter for the FR-Algorithm. optimum distance between connected vertices
   * @param iterations Number of iterations to perform
   * @param width Width of the layouting-space
   * @param height Height of the layouting-space
   */
  public FRLayouterNaive(double k, int iterations, int width, int height) {
    super(k, iterations, width, height, 1);
  }

  @Override
  public DataSet<Tuple3<GradoopId, Double, Double>> repulsionForces(DataSet<Vertex> vertices) {
    return vertices.cross(vertices).with(new FRRepulsionFunction(k));
  }
}
