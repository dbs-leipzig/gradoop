/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.representation.transactional.traversalcode;

import java.io.Serializable;

/**
 * DFSStep of an embedding.
 *
 * @param <C> vertex and edge value type
 */
public class Traversal<C extends Comparable<C>>
  implements Serializable, Comparable<Traversal<C>> {

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
  public Traversal(
    int fromTime, C fromValue, boolean outgoing, C edgeValue, int toTime,  C toValue) {
    this.fromTime = fromTime;
    this.fromValue = fromValue;
    this.outgoing = outgoing;
    this.edgeValue = edgeValue;
    this.toTime = toTime;
    this.toValue = toValue;
  }

  public boolean isBackwards() {
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

    Traversal<?> that = (Traversal<?>) o;

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

  /**
   * Comparison according to gSpan lexicographic order.
   *
   * @param that other extension
   * @return comparison result
   */
  @Override
  public int compareTo(Traversal<C> that) {

    int comparison;

    // no difference is times
    if (this.fromTime == that.fromTime && this.toTime == that.toTime) {

      // compare from values
      comparison = this.fromValue.compareTo(that.fromValue);

      if (comparison == 0) {

        // compare direction
        boolean thisIsOutgoing = this.isOutgoing();
        boolean thatIsOutgoing = that.isOutgoing();

        if (thisIsOutgoing && !thatIsOutgoing) {
          comparison = -1;
        } else if (thatIsOutgoing && !thisIsOutgoing) {
          comparison = 1;
        } else {

          // compare edge values
          comparison = this.edgeValue.compareTo(that.edgeValue);

          if (comparison == 0) {

            // compare to values
            comparison = this.toValue.compareTo(that.toValue);
          }
        }
      }

      // time differences
    } else {

      // compare backtracking
      boolean thisIsBacktrack = this.isBackwards();
      boolean thatIsBacktrack = that.isBackwards();

      if (thisIsBacktrack && !thatIsBacktrack) {
        comparison = -1;
      } else if (thatIsBacktrack && !thisIsBacktrack) {
        comparison = 1;
      } else {

        // both forwards
        if (thatIsBacktrack) {

          // back to earlier vertex is smaller
          comparison = this.toTime - that.toTime;

          // both backwards
        } else {

          // forwards from later vertex is smaller
          comparison = that.fromTime - this.fromTime;
        }
      }
    }

    return comparison;
  }

  public boolean isForwards() {
    return !isBackwards();
  }
}
