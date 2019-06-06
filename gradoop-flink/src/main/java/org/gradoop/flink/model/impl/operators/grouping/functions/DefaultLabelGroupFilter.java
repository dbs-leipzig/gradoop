/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

/**
 * Checks whether a vertex is a member of the default {@link LabelGroup}.
 *
 * @param <V> vertex type.
 */
public class DefaultLabelGroupFilter<V extends EPGMVertex> extends LabelGroupFilter<V> {

  /**
   * True, if grouping by labels
   */
  private final boolean groupingByLabels;

  /**
   * LabelGroup to check
   */
  private final LabelGroup group;

  /**
   * Constructor
   *
   * @param groupingByLabels true, if grouping by labels
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
