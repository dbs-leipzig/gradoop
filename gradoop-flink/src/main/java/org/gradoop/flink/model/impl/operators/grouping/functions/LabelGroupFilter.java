package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

public abstract class LabelGroupFilter<V extends EPGMVertex> implements FilterFunction<V> {

  /**
   * Check whether a vertex exhibits all properties of a  {@link LabelGroup}.
   *
   * @param group  labelGroup to check
   * @param vertex vertex to check
   * @return true, if a vertex exhibits all properties or if grouped by properties (propertyKeys)
   * are empty.
   */
  boolean hasVertexAllPropertiesOfGroup(LabelGroup group, V vertex) {
    return group.getPropertyKeys()
      .parallelStream()
      .allMatch(vertex::hasProperty);
  }
}
