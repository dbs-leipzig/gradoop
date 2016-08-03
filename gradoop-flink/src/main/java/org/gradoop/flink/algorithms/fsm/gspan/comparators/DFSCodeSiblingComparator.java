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

/**
 * Comparator for DFS codes which are siblings in a code tree.
 */
public class DFSCodeSiblingComparator
  implements Comparator<DFSCode>, Serializable {

  /**
   * DFS step comparator
   */
  private final DFSStepComparator stepComparator;

  /**
   * Constructor
   *
   * @param directed true, if comparison should be done for directed graphs,
   *                 false, for undirected
   */
  public DFSCodeSiblingComparator(boolean directed) {
    stepComparator = new DFSStepComparator(directed);
  }

  @Override
  public int compare(DFSCode c1, DFSCode c2) {

    DFSStep e1 = c1.getLastStep();
    DFSStep e2 = c2.getLastStep();

    return stepComparator.compare(e1, e2);
  }
}
