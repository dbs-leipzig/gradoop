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

import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator of DFS code steps based on gSpan lexicographical order.
 */
public class DFSStepComparator implements Comparator<DFSStep>, Serializable {

  /**
   * true for comparing DFS steps of directed graphs,
   * false for undirected graphs
   */
  private final boolean directed;

  /**
   * constructor
   * @param directed true for comparing DFS steps of directed graphs
   */
  public DFSStepComparator(boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(DFSStep e1, DFSStep e2) {
    int comparison;

    if (e1.isForward()) {
      if (e2.isBackward()) {
        // forward - backward
        comparison = 1;
      } else {
        // forward - forward
        if (e1.getFromTime() == e2.getFromTime()) {
          // forward from same vertex
          comparison = compareLabels(e1, e2);
        } else if (e1.getFromTime() > e2.getFromTime()) {
          // first forward from later vertex
          comparison = -1;
        } else  {
          // second forward from later vertex
          comparison = 1;
        }
      }
    } else {
      if (e2.isForward()) {
        // backward - forward
        comparison = -1;
      } else {
        // backward - backward
        if (e1.getToTime() == e2.getToTime()) {
          // backward to same vertex
          comparison = compareLabels(e1, e2);
        } else if (e1.getToTime() < e2.getToTime()) {
          // first back to earlier vertex
          comparison = -1;
        } else {
          // second back to earlier vertex
          comparison = 1;
        }
      }
    }

    return comparison;
  }

  /**
   * extracted method to compare DFS steps based on start, edge and edge labels
   * as well as the traversal direction
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return comparison result
   */
  private int compareLabels(DFSStep s1, DFSStep s2) {
    int comparison;

    if (s1.getFromLabel().compareTo(s2.getFromLabel()) < 0) {
      comparison = -1;
    } else if (s1.getFromLabel().compareTo(s2.getFromLabel()) > 0) {
      comparison = 1;
    } else {
      if (directed) {
        comparison = compareDirectedLabels(s1, s2);
      } else {
        comparison = compareUndirectedLabels(s1, s2);
      }
    }
    return comparison;
  }

  /**
   * label comparison for directed mode
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return  comparison result
   */
  private int compareDirectedLabels(DFSStep s1, DFSStep s2) {
    int comparison;

    if (s1.isOutgoing() && !s2.isOutgoing()) {
      comparison = -1;
    } else if (!s1.isOutgoing() && s2.isOutgoing()) {
      comparison = 1;
    } else {
      comparison = compareUndirectedLabels(s1, s2);
    }
    return comparison;
  }

  /**
   * label comparison for undirected mode
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return  comparison result
   */
  private int compareUndirectedLabels(DFSStep s1, DFSStep s2) {
    int comparison;
    if (s1.getEdgeLabel().compareTo(s2.getEdgeLabel()) < 0) {
      comparison = -1;
    } else if (s1.getEdgeLabel().compareTo(s2.getEdgeLabel()) > 0) {
      comparison = 1;
    } else {
      if (s1.getToLabel().compareTo(s2.getToLabel()) < 0) {
        comparison = -1;
      } else if (s1.getToLabel().compareTo(s2.getToLabel()) > 0) {
        comparison = 1;
      } else {
        comparison = 0;
      }
    }
    return comparison;
  }

}
