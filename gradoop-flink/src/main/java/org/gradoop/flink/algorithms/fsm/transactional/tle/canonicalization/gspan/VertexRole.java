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

package org.gradoop.flink.algorithms.fsm.transactional.tle.canonicalization.gspan;

/**
 * Roles of a vertex with regard to an embedding.
 */
public class VertexRole  {
  /**
   * not contained
   */
  public static final int NOT_CONTAINED = 0;
  /**
   * contained
   */
  public static final int CONTAINED = 1;
  /**
   * on rightmost path
   */
  public static final int ON_RIGHTMOST_PATH = 2;
  /**
   * is rightmost
   */
  public static final int IS_RIGHTMOST = 3;
}
