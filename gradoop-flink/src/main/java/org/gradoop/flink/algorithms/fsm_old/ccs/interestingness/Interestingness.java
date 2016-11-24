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

package org.gradoop.flink.algorithms.fsm_old.ccs.interestingness;

import java.io.Serializable;

/**
 * Encapsulates the interestingness measure used for characteristic patterns.
 */
public class Interestingness implements Serializable {

  /**
   * interestingness threshold
   */
  private final float minInterestingness;

  /**
   * Constructor.
   *
   * @param minInterestingness interestingness threshold
   */
  public Interestingness(float minInterestingness) {
    this.minInterestingness = minInterestingness;
  }

  /**
   * Returns true, if interesting.
   *
   * @param categorySupport support within a category
   * @param avgSupport average support over all categories
   * @return interesting or not
   */
  public boolean isInteresting(float categorySupport, float avgSupport) {

    boolean interesting;

    if (categorySupport == 0) {
      interesting = false;
    } else if (avgSupport == 0) {
      interesting = true;
    } else {
      interesting = categorySupport / avgSupport >= minInterestingness;
    }

    return interesting;
  }
}
