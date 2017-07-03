
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
