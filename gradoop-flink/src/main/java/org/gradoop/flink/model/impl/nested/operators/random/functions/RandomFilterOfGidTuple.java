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

package org.gradoop.flink.model.impl.nested.operators.random.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Specific instance for (GradoopId,GradoopId)
 */
public class RandomFilterOfGidTuple extends RandomFilter<Tuple2<GradoopId, GradoopId>> {
  /**
   * Creates a new filter instance.
   *
   * @param sampleSize relative sample size
   * @param randomSeed random seed (can be {@code} null)
   */
  public RandomFilterOfGidTuple(float sampleSize, long randomSeed) {
    super(sampleSize, randomSeed);
  }
}
