
package org.gradoop.flink.algorithms.fsm.transactional.tle.pojos;

/**
 * Represents an edge within Subgraph embeddings.
 */
public class FSMEdge {

  /**
   * source vertex id
   */
  private final int sourceId;
  /**
   * edge label
   */
  private final String label;
  /**
   * target vertex id
   */
  private final int targetId;

  /**
   * Constructor.
   *
   * @param sourceId source vertex id
   * @param label edge label
   * @param targetId target vertex id
   */
  public FSMEdge(int sourceId, String label, int targetId) {
    this.sourceId = sourceId;
    this.label = label;
    this.targetId = targetId;
  }

  public int getSourceId() {
    return sourceId;
  }

  public int getTargetId() {
    return targetId;
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

    if (sourceId != that.sourceId) {
      return false;
    }
    if (targetId != that.targetId) {
      return false;
    }
    return label.equals(that.label);

  }

  @Override
  public int hashCode() {
    int result = sourceId;
    result = 31 * result + label.hashCode();
    result = 31 * result + targetId;
    return result;
  }

  @Override
  public String toString() {
    return sourceId + "-" + label + "->" + targetId;
  }
}
