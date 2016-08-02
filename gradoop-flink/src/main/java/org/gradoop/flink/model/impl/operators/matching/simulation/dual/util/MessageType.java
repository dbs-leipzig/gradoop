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

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.util;

import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples
  .Deletion;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples
  .Message;

/**
 * Message types used in {@link Deletion} and {@link Message}.
 */
public enum MessageType {
  /**
   * Message is sent from the vertex to itself.
   */
  FROM_SELF,
  /**
   * Message is sent from a child vertex.
   */
  FROM_CHILD,
  /**
   * Message is sent from a parent vertex.
   */
  FROM_PARENT,
  /**
   * Message is sent from a child vertex which will be removed at the end of
   * the iteration.
   */
  FROM_CHILD_REMOVE,
  /**
   * Message is sent from a parent vertex which will be removed at the end of
   * the iteration.
   */
  FROM_PARENT_REMOVE
}
