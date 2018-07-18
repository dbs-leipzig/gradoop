/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.transactional.tle.interestingness;

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
