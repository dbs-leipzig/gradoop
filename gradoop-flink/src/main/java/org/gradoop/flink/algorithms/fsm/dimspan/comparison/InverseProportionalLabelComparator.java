
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Frequency-based label comparator where higher frequency is smaller.
 */
public class InverseProportionalLabelComparator implements LabelComparator {

  @Override
  public int compare(WithCount<String> a, WithCount<String> b) {
    int comparison;

    if (a.getCount() > b.getCount()) {
      comparison = -1;
    } else if (a.getCount() < b.getCount()) {
      comparison = 1;
    } else {
      comparison = a.getObject().compareTo(b.getObject());
    }

    return comparison;
  }
}
