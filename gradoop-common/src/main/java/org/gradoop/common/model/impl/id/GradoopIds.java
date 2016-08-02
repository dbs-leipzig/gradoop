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

package org.gradoop.common.model.impl.id;

/**
 * Static helper functions for GradoopIds
 */
public class GradoopIds {

  /**
   * Compares the given GradoopIds and returns the smaller one. It both are
   * equal, the first argument is returned.
   *
   * @param id1 first GradoopID
   * @param id2 second GradoopID
   * @return smaller GradoopId or first if equal
   */
  public static GradoopId min(GradoopId id1, GradoopId id2) {
    int comparison = id1.compareTo(id2);
    return comparison == 0 ? id1 : (comparison == -1 ? id1 : id2);
  }
}
