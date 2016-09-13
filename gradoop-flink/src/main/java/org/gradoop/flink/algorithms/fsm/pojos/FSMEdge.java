package org.gradoop.flink.algorithms.fsm.pojos;

public class FSMEdge {

  private final int source;
  private final String label;
  private final int target;

  public FSMEdge(int source, String label, int target) {
    this.source = source;
    this.label = label;
    this.target = target;
  }

  public int getSourceId() {
    return source;
  }

  public int getTargetId() {
    return target;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FSMEdge that = (FSMEdge) o;

    if (source != that.source) {
      return false;
    }
    if (target != that.target) {
      return false;
    }
    return label.equals(that.label);

  }

  @Override
  public int hashCode() {
    int result = source;
    result = 31 * result + label.hashCode();
    result = 31 * result + target;
    return result;
  }

  @Override
  public String toString() {
    return source + "-" + label + "->" + target;
  }
}
