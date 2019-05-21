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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/** Applies forces to vertices.
 *
 */
public class FRForceApplicator extends
  RichJoinFunction<Vertex, Tuple3<GradoopId, Double, Double>, Vertex> {
  /** Width of the layouting-space */
  private int width;
  /** Height of the layouting-space */
  private int height;
  /** Parameter for FR-Algorithm. Here used for simulated annealing */
  private double k;
  /** Number of FR-iterations to perform. Here used for simulated annealing */
  private int maxIterations;

  /** Create new FRForceApplicator
   *
   * @param width The width of the layouting-space
   * @param height The height of the layouting-space
   * @param k A parameter of the FR-Algorithm.
   * @param maxIterations Number of iterations the FR-Algorithm will have
   */
  public FRForceApplicator(int width, int height, double k, int maxIterations) {
    this.width = width;
    this.height = height;
    this.k = k;
    this.maxIterations = maxIterations;
  }

  @Override
  public Vertex join(Vertex first, Tuple3<GradoopId, Double, Double> second) throws Exception {
    int iteration = getIterationRuntimeContext().getSuperstepNumber();
    double temp = k / 2.0;
    double speedLimit = -(temp / maxIterations) * iteration + temp + (k / 10.0);

    Vector movement = Vector.fromForceTuple(second);
    movement = movement.clamped(speedLimit);

    Vector position = Vector.fromVertexPosition(first);
    position = position.add(movement);
    position = position.confined(0, width - 1, 0, height - 1);

    position.setVertexPosition(first);
    return first;
  }

}
