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

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

/**
 * Represents an Edge with an extracted tie point
 *
 * f0 -> edge join key
 * f1 -> edge id
 * f2 -> edge expand key
 */
public class EdgeWithTiePoint extends Tuple3<GradoopId, GradoopId, GradoopId> {

  /**
   * Creates an empty Object
   */
  public EdgeWithTiePoint() {
  }

  /**
   * Creates a new Edge with extracted tie point from the given edge
   * @param edge edge embedding
   */
  public EdgeWithTiePoint(Embedding edge) {
    this.f0 = edge.getId(0);
    this.f1 = edge.getId(1);
    this.f2 = edge.getId(2);
  }

  /**
   * Set source id
   * @param id source id
   */
  public void setSource(GradoopId id) {
    f0 = id;
  }

  /**
   * Get source id
   * @return source id
   */
  public GradoopId getSource() {
    return f0;
  }

  /**
   * Set edge id
   * @param id edge id
   */
  public void setId(GradoopId id) {
    f1 = id;
  }

  /**
   * Get edge id
   * @return edge id
   */
  public GradoopId getId() {
    return f1;
  }

  /**
   * Set target id
   * @param id target id
   */
  public void setTarget(GradoopId id) {
    f2 = id;
  }

  /**
   * Get target id
   * @return target id
   */
  public GradoopId getTarget() {
    return f2;
  }
}
