/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Compares two vertices based on their position and the forces acting on them
 */
public class DefaultVertexCompareFunction implements VertexCompareFunction {
  /**
   * Parameter k that is used in the FR-Algorithm
   */
  protected double k;

  /**
   * Construct new compare-function
   *
   * @param k Parameter k that is used in the FR-Algorithm. Is the distance between two connected
   *          vertices where attraction and repulsion are equal.
   */
  public DefaultVertexCompareFunction(double k) {
    this.k = k;
  }

  @Override
  public double compare(LVertex v1, LVertex v2) {
    // compare position-difference in relation to k. Cap values at 0,1.
    double positionSimilarity =
      Math.min(1, Math.max(0, 1 - ((v1.getPosition().distance(v2.getPosition()) - k) / k)));

    // compare difference in forces. Not only consider direction but als magnitude.
    Vector force1 = v1.getForce().div(v1.getCount());
    Vector force2 = v2.getForce().div(v2.getCount());
    double forceSimilarity =
      1 - (force1.distance(force2) / (force1.magnitude() + force2.magnitude()));

    return positionSimilarity * forceSimilarity;
  }
}
