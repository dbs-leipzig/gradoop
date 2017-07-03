
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import org.gradoop.flink.model.impl.tuples.WithCount;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Frequency-based label comparator.
 */
public interface LabelComparator extends Comparator<WithCount<String>>, Serializable {
}
