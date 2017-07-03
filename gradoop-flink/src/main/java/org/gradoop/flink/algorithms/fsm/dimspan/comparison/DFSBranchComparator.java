
package org.gradoop.flink.algorithms.fsm.dimspan.comparison;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare initial extensions of DFS codes.
 */
public interface DFSBranchComparator extends Comparator<int[]>, Serializable {
}
