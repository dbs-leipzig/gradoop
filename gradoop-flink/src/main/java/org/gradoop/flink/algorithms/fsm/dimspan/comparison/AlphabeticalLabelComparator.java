
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Label comparator ignoring their frequency.
 */
public class AlphabeticalLabelComparator implements LabelComparator {

  @Override
  public int compare(WithCount<String> a, WithCount<String> b) {
    return a.getObject().compareTo(b.getObject());
  }
}
