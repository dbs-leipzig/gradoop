package org.gradoop.model.inmemory;

import org.gradoop.model.Edge;

import java.util.Map;

/**
 * Transient representation of an edge.
 */
public class MemoryEdge extends SingleLabeledPropertyContainer implements Edge {
  private final Long otherID;

  private final Long index;

  protected MemoryEdge(Long otherID, String label, Long index,
                       Map<String, Object> properties) {
    super(label, properties);
    if (otherID == null) {
      throw new IllegalArgumentException("otherID must not be null");
    }
    if (index == null) {
      throw new IllegalArgumentException("index must not be null");
    }
    this.otherID = otherID;
    this.index = index;
  }

  @Override
  public Long getOtherID() {
    return this.otherID;
  }

  @Override
  public Long getIndex() {
    return this.index;
  }
}
