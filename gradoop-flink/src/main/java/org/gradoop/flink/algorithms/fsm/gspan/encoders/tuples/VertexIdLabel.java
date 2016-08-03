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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * (vertexId, vertexLabel)
 */
public class VertexIdLabel extends Tuple2<GradoopId, Integer> {

  /**
   * Default constructor.
   */
  public VertexIdLabel() {
  }

  /**
   * Valued constructor.
   * @param id vertex id
   * @param label vertex label
   */
  public VertexIdLabel(GradoopId id, Integer label) {
    super(id, label);
  }

  public Integer getLabel() {
    return this.f1;
  }
}
