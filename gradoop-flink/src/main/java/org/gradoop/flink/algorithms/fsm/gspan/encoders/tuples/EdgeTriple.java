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

/**
 * Describe the minimum features of an edge triple representation required to
 * build a gSpan graph representation.
 *
 * @param <T> Id type
 */
public interface  EdgeTriple<T> {

  /**
   * Getter.
   * @return edge label
   */
  Integer getEdgeLabel();

  /**
   * Getter.
   * @return source vertex id
   */
  T getSourceId();

  /**
   * Getter.
   * @return source vertex label
   */
  Integer getSourceLabel();

  /**
   * Getter.
   * @return target vertex id
   */
  T getTargetId();

  /**
   * Getter.
   * @return target vertex label
   */
  Integer getTargetLabel();
}
