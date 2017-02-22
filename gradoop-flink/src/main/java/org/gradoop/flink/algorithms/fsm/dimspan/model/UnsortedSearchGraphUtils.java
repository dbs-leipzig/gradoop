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

package org.gradoop.flink.algorithms.fsm.dimspan.model;

/**
 * Util methods to interpret and manipulate unsorted int-array encoded graphs
 */
public class UnsortedSearchGraphUtils extends SearchGraphUtilsBase implements SearchGraphUtils {

  @Override
  public int getFirstGeqEdgeId(int[] graphMux, int[] searchMux, int searchFromEdgeId) {
    return 0;
  }

}
