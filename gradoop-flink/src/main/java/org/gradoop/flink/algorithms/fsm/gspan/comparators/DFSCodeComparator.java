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

package org.gradoop.flink.algorithms.fsm.gspan.comparators;

import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Step-wise comparator for DFS codes.
 */
public class DFSCodeComparator implements Comparator<DFSCode>, Serializable {

  /**
   * step comparator
   */
  private final DFSStepComparator dfsStepComparator;

  /**
   * constructor
   * @param directed true for comparing DFS codes of directed graphs
   */
  public DFSCodeComparator(boolean directed) {
    dfsStepComparator = new DFSStepComparator(directed);
  }

  @Override
  public int compare(DFSCode c1, DFSCode c2) {
    int comparison = 0;

    Iterator<DFSStep> i1 = c1.getSteps().iterator();
    Iterator<DFSStep> i2 = c2.getSteps().iterator();

    // compare steps until there is a difference
    while (comparison == 0 && i1.hasNext() && i2.hasNext()) {
      DFSStep s1 = i1.next();
      DFSStep s2 = i2.next();

      comparison = dfsStepComparator.compare(s1, s2);
    }

    // common parent
    if (comparison == 0) {
      // c1 is child of c2
      if (i1.hasNext()) {
        comparison = 1;
        // c2 is child of c1
      } else if (i2.hasNext()) {
        comparison = -1;
      }
    }

    return comparison;
  }
}
