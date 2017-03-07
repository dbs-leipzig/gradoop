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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;

/**
 * Represents an Edge with an extracted tie point
 *
 * f0 -> edge join key
 * f1 -> edge
 */
public class EdgeWithTiePoint extends Tuple2<GradoopId, Embedding> {

  /**
   * Creates an empty Object
   */
  public EdgeWithTiePoint() {
    super();
  }

  /**
   * Creates a new Edge with extracted tie point from the given edge
   * @param edge edge embedding
   */
  public EdgeWithTiePoint(Embedding edge) {
    this.f0 = edge.getId(0);
    this.f1 = edge;
  }
}
