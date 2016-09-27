package org.gradoop.flink.algorithms.fsm.common.canonicalization.pojos;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class DFSExtension implements Comparable<DFSExtension> {

  private static final char VERTEX_LABEL_SEPARATOR = ',';
  private static final char OUTGOING_CHAR = '>';
  private static final char INCOMING_CHAR = '<';
  private static final char EDGE_END_CHAR = '-';

  private final int fromTime;
  private final String fromLabel;
  private final boolean outgoing;
  private final String edgeLabel;
  private final int toTime;
  private final String toLabel;

  public DFSExtension(int fromTime, String fromLabel, boolean outgoing,
    String edgeLabel, int toTime, String toLabel) {
    this.fromTime = fromTime;
    this.fromLabel = fromLabel;
    this.outgoing = outgoing;
    this.edgeLabel = edgeLabel;
    this.toTime = toTime;
    this.toLabel = toLabel;
  }

  @Override
  public int compareTo(DFSExtension that) {

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

  private int compareEdgeDirectionAndLabel(DFSExtension that) {
    int comparison;

    if (this.outgoing && ! that.outgoing) {
      comparison = -1;
    } else if (! this.outgoing && that.outgoing) {
      comparison = 1;
    } else {
      comparison = this.edgeLabel.compareTo(that.edgeLabel);
    }

    return comparison;
  }
}
