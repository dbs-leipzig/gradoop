package org.gradoop.model.inmemory;

import org.gradoop.GConstants;
import org.gradoop.model.Edge;

import java.util.Map;

/**
 * Transient representation of an edge.
 */
public class MemoryEdge extends SingleLabeledPropertyContainer implements Edge {
  private final Long otherID;

  private final Long index;

  public MemoryEdge(final Long otherID, final Long index) {
    super(GConstants.DEFAULT_EDGE_LABEL, null);
    checkID(otherID);
    checkIndex(index);
    this.otherID = otherID;
    this.index = index;
  }

  public MemoryEdge(final Long otherID, final String label, final Long index,
                    final Map<String, Object> properties) {
    super(label, properties);
    checkID(otherID);
    checkIndex(index);
    this.otherID = otherID;
    this.index = index;
  }

  private void checkID(Long otherID) {
    if (otherID == null) {
      throw new IllegalArgumentException("otherID must not be null");
    }
  }

  private void checkIndex(Long index) {
    if (index == null) {
      throw new IllegalArgumentException("index must not be null");
    }
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
