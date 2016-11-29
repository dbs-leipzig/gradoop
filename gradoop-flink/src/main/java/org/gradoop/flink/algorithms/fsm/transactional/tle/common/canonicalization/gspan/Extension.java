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

package org.gradoop.flink.algorithms.fsm.transactional.tle.common.canonicalization.gspan;

/**
 * Extension of an embedding.
 */
public class Extension implements Comparable<Extension> {

  /**
   * separator between vertex time and label
   */
  private static final char TIME_LABEL_SEPARATOR = ':';
  /**
   * outgoing edge start
   */
  private static final char OUTGOING_CHAR = '>';
  /**
   * incoming edge start
   */
  private static final char INCOMING_CHAR = '<';
  /**
   * edge start of undirected and end of all edges
   */
  private static final char EDGE_CHAR = '-';

  /**
   * extension start time
   */
  private final int fromTime;
  /**
   * extension start label
   */
  private final String fromLabel;
  /**
   * outgoing or incoming traversal
   */
  private final boolean outgoing;
  /**
   * edge label
   */
  private final String edgeLabel;
  /**
   * traversal end time
   */
  private final int toTime;
  /**
   * traversal end label
   */
  private final String toLabel;
  /**
   * directed mode
   */
  private final boolean directed;

  /**
   * Constructor
   * @param fromTime    extension start time
   * @param fromLabel   extension start label
   * @param outgoing    outgoing or incoming traversal
   * @param edgeLabel   edge label
   * @param toTime      traversal end time
   * @param toLabel     traversal end label
   * @param directed    directed mode
   */
  public Extension(int fromTime, String fromLabel, boolean outgoing,
    String edgeLabel, int toTime, String toLabel, boolean directed) {
    this.fromTime = fromTime;
    this.fromLabel = fromLabel;
    this.outgoing = outgoing;
    this.edgeLabel = edgeLabel;
    this.toTime = toTime;
    this.toLabel = toLabel;
    this.directed = directed;
  }

  @Override
  public int compareTo(Extension that) {

    int comparison;

    boolean thisBackward = this.toTime <= this.fromTime;
    boolean thisForward = !thisBackward;
    boolean thatBackward = that.toTime <= that.fromTime;
    boolean thatForward = !thatBackward;

    if (thisBackward && thatForward) {
      comparison = -1;
    } else if (thisForward && thatBackward) {
      comparison = 1;
    } else if (thisBackward) {
      // both backward

      if (this.toTime < that.toTime) {
        comparison = -1;
      } else if (this.toTime > that.toTime) {
        comparison = 1;
      } else {
        comparison = compareEdgeDirectionAndLabel(that);
      }

    } else {
      // both forward

      if (this.fromTime > that.fromTime) {
        comparison = -1;
      } else if (this.fromTime < that.fromTime) {
        comparison = 1;
      } else {
        comparison = compareEdgeDirectionAndLabel(that);

        if (comparison == 0) {
          comparison = this.toLabel.compareTo(that.toLabel);
        }
      }
    }

    return comparison;
  }

  /**
   * compares edge direction and label of two extensions
   *
   * @param that other extension
   *
   * @return comparison result
   */
  private int compareEdgeDirectionAndLabel(Extension that) {
    int comparison;

    if (!directed || this.outgoing == that.outgoing) {
      comparison = this.edgeLabel.compareTo(that.edgeLabel);
    } else if (this.outgoing) {
      comparison = -1;
    } else {
      comparison = 1;
    }

    return comparison;
  }

  @Override
  public String toString() {
    return String.valueOf(fromTime) + TIME_LABEL_SEPARATOR + fromLabel +
      (directed ? (outgoing ? OUTGOING_CHAR : INCOMING_CHAR) : EDGE_CHAR) +
      edgeLabel + EDGE_CHAR +
      toTime + TIME_LABEL_SEPARATOR + toLabel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Extension extension = (Extension) o;

    if (fromTime != extension.fromTime) {
      return false;
    }
    if (outgoing != extension.outgoing) {
      return false;
    }
    if (toTime != extension.toTime) {
      return false;
    }
    if (fromLabel != null ? !fromLabel.equals(extension.fromLabel) :
      extension.fromLabel != null) {
      return false;
    }
    if (edgeLabel != null ? !edgeLabel.equals(extension.edgeLabel) :
      extension.edgeLabel != null) {
      return false;
    }
    return toLabel != null ? toLabel.equals(extension.toLabel) :
      extension.toLabel == null;

  }

  @Override
  public int hashCode() {
    int result = fromTime;
    result = 31 * result + (fromLabel != null ? fromLabel.hashCode() : 0);
    result = 31 * result + (outgoing ? 1 : 0);
    result = 31 * result + (edgeLabel != null ? edgeLabel.hashCode() : 0);
    result = 31 * result + toTime;
    result = 31 * result + (toLabel != null ? toLabel.hashCode() : 0);
    return result;
  }
}
