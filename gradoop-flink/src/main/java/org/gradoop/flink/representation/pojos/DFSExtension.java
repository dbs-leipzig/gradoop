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

package org.gradoop.flink.representation.pojos;

import java.io.Serializable;

/**
 * DFSStep of an embedding.
 */
public class DFSExtension<C extends Comparable<C>> implements Serializable {

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
  private final C fromValue;
  /**
   * outgoing or incoming traversal
   */
  private final boolean outgoing;
  /**
   * edge label
   */
  private final C edgeValue;
  /**
   * traversal end time
   */
  private final int toTime;
  /**
   * traversal end label
   */
  private final C toValue;

  /**
   * Constructor
   * @param fromTime    extension start time
   * @param fromValue   extension start label
   * @param outgoing    outgoing or incoming traversal
   * @param edgeValue   edge label
   * @param toTime      traversal end time
   * @param toValue     traversal end label
   */
  public DFSExtension(
    int fromTime, C fromValue, boolean outgoing, C edgeValue, int toTime,  C toValue) {
    this.fromTime = fromTime;
    this.fromValue = fromValue;
    this.outgoing = outgoing;
    this.edgeValue = edgeValue;
    this.toTime = toTime;
    this.toValue = toValue;
  }

  public boolean isBackTrack() {
    return toTime <= fromTime;
  }

  public boolean isLoop() {
    return fromTime == toTime;
  }

  public int getFromTime() {
    return fromTime;
  }

  public C getFromValue() {
    return fromValue;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  public C getEdgeValue() {
    return edgeValue;
  }

  public int getToTime() {
    return toTime;
  }

  public C getToValue() {
    return toValue;
  }

  @Override
  public String toString() {
    return String.valueOf(fromTime) + TIME_LABEL_SEPARATOR + fromValue +
      (outgoing ? OUTGOING_CHAR : INCOMING_CHAR) + edgeValue + EDGE_CHAR +
      toTime + TIME_LABEL_SEPARATOR + toValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DFSExtension<?> that = (DFSExtension<?>) o;

    if (fromTime != that.fromTime) {
      return false;
    }
    if (outgoing != that.outgoing) {
      return false;
    }
    if (toTime != that.toTime) {
      return false;
    }
    if (!fromValue.equals(that.fromValue)) {
      return false;
    }
    if (!edgeValue.equals(that.edgeValue)) {
      return false;
    }
    return toValue.equals(that.toValue);

  }

  @Override
  public int hashCode() {
    int result = fromTime;
    result = 31 * result + fromValue.hashCode();
    result = 31 * result + (outgoing ? 1 : 0);
    result = 31 * result + edgeValue.hashCode();
    result = 31 * result + toTime;
    result = 31 * result + toValue.hashCode();
    return result;
  }

  public int compareTo(DFSExtension<C> that) {
    return 0;
  }
}
