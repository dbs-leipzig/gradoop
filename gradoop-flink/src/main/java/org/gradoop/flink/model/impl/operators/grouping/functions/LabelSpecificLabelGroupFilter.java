package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

/**
 * Checks whether a vertex is a member of a {@link LabelGroup} when using label specific grouping.
 *
 * @param <V> vertex type.
 */
public class LabelSpecificLabelGroupFilter<V extends EPGMVertex> extends LabelGroupFilter<V> {
  private final LabelGroup group;

  /**
   * Constructor
   *
   * @param group group to check.
   */
  public LabelSpecificLabelGroupFilter(LabelGroup group) {
    this.group = group;
  }

  @Override
  public boolean filter(V vertex) throws Exception {
    return vertex.getLabel().equals(group.getGroupingLabel()) &&
      hasVertexAllPropertiesOfGroup(group, vertex);
  }
}
