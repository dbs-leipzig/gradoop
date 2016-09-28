package org.gradoop.flink.algorithms.fsm.common.canonicalization.gspan;

public class Extension implements Comparable<Extension> {

  private static final char VERTEX_LABEL_SEPARATOR = ':';
  private static final char OUTGOING_CHAR = '>';
  private static final char INCOMING_CHAR = '<';
  private static final char EDGE_CHAR = '-';

  private final int fromTime;
  private final String fromLabel;
  private final boolean outgoing;
  private final String edgeLabel;
  private final int toTime;
  private final String toLabel;
  private final boolean directed;

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
    return String.valueOf(fromTime) + VERTEX_LABEL_SEPARATOR + fromLabel +
      (directed ? (outgoing ? OUTGOING_CHAR : INCOMING_CHAR) : EDGE_CHAR) +
      edgeLabel + EDGE_CHAR +
      toTime + VERTEX_LABEL_SEPARATOR + toLabel;
  }
}
