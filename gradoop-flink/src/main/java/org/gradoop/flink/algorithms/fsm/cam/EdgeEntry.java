package org.gradoop.flink.algorithms.fsm.cam;

public class EdgeEntry {

  private final int edgeId;
  private final boolean outgoing;

  public EdgeEntry(int edgeId, boolean outgoing) {
    this.edgeId = edgeId;
    this.outgoing = outgoing;
  }

  public int getEdgeId() {
    return edgeId;
  }

  public boolean isOutgoing() {
    return outgoing;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EdgeEntry that = (EdgeEntry) o;

    if (edgeId != that.edgeId) {
      return false;
    }
    return outgoing == that.outgoing;
  }

  @Override
  public int hashCode() {
    return outgoing ? edgeId : edgeId * -1;
  }
}
