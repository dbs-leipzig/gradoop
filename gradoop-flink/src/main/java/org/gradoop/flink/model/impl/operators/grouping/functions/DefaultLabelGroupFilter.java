package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

/**
 * Checks whether a vertex is a member of the default {@link LabelGroup}.
 *
 * @param <V> vertex type.
 */
public class DefaultLabelGroupFilter<V extends EPGMVertex> extends LabelGroupFilter<V> {

  private final boolean groupingByLabels;
  private final LabelGroup group;

  /**
   * Constructor
   *
   * @param groupingByLabels true, if grouping by labels, see
   *                         {@link Grouping.GroupingBuilder#useVertexLabel(boolean)}.
   * @param group            group to check
   */
  public DefaultLabelGroupFilter(boolean groupingByLabels, LabelGroup group) {
    this.groupingByLabels = groupingByLabels;
    this.group = group;
  }

  @Override
  public boolean filter(V vertex) throws Exception {

    if (groupingByLabels) {

      // if the vertex's label is empty,
      // a) and the group by properties are empty => vertex is not member of the group
      // b) and group by properties are not empty => vertex can be member of the group
      if (vertex.getLabel().isEmpty()) {
        return !group.getPropertyKeys().isEmpty() && hasVertexAllPropertiesOfGroup(group, vertex);
      } else {
        return hasVertexAllPropertiesOfGroup(group, vertex);
      }

    } else {

      // if we are not grouping by labels, we need to check if the vertex has all
      // properties grouped by
      // if there is no grouped by property, no vertex should be part of the group

      return !group.getPropertyKeys().isEmpty() && hasVertexAllPropertiesOfGroup(group, vertex);
    }
  }
}
