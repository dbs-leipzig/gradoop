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

package org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos;

import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Describe a FSM-fitted graph representation.
 */
public interface FSMGraph {

  /**
   * Getter.
   *
   * @return id-vertex label map
   */
  Map<Integer, String> getVertices();

  /**
   * Setter.
   *
   * @return id-edge map
   */
  Map<Integer, FSMEdge> getEdges();

  /**
   * Getter.
   *
   * @return graph id
   */
  GradoopId getId();
}
