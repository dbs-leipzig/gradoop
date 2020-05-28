/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Function to check whether a vertex is a member of a {@link LabelGroup}.
 *
 * @param <V> The vertex type.
 */
public class LabelGroupFilter<V extends Vertex> implements FilterFunction<V> {

  /**
   * LabelGroups to check.
   */
  private final List<LabelGroup> labelGroups;

  /**
   * A flag to identify if grouping by vertex label is enabled.
   */
  private final boolean groupingByLabels;

  /**
   * A filter function to retain vertices that are in a label group which means, the vertex has a
   * label or property that matches a grouping key from the config.
   *
   * @param labelGroups the label groups used by grouping
   * @param groupingByLabels a flag that indicates if the grouping by all labels is enabled
   */
  public LabelGroupFilter(List<LabelGroup> labelGroups, boolean groupingByLabels) {
    this.labelGroups = Objects.requireNonNull(labelGroups);
    this.groupingByLabels = groupingByLabels;
  }

  /**
   * Check whether a vertex exhibits all properties of a {@link LabelGroup}.
   *
   * @param group  labelGroup to check
   * @param vertex vertex to check
   * @return true, if a vertex exhibits all properties or if grouped by properties (propertyKeys)
   * are empty.
   */
  private boolean hasVertexAllPropertiesOfGroup(LabelGroup group, V vertex) {
    return group.getPropertyKeys().parallelStream().allMatch(vertex::hasProperty);
  }

  @Override
  public boolean filter(V vertex) throws Exception {
    boolean vertexIsInALabelGroup = false;
    LabelGroup labelGroup;

    final Iterator<LabelGroup> labelGroupIterator = labelGroups.iterator();

    // Iterate over all label labelGroups until vertex matches a label group
    while (labelGroupIterator.hasNext() && !vertexIsInALabelGroup) {
      labelGroup = labelGroupIterator.next();

      // Default case (parameter group is not label specific)
      if (labelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP)) {

        if (groupingByLabels) {

          // if the vertex's label is empty,
          // a) and the group by properties are empty => vertex is not member of the group
          // b) and group by properties are not empty => vertex can be member of the group
          if (!vertex.getLabel().isEmpty()) {
            vertexIsInALabelGroup = hasVertexAllPropertiesOfGroup(labelGroup, vertex);
          }
        }
        // if we not group by labels, we need to check if the vertex has all properties grouped by
        // if there is no grouped by property, no vertex should be part of the group
        vertexIsInALabelGroup = vertexIsInALabelGroup || !labelGroup.getPropertyKeys().isEmpty() &&
          hasVertexAllPropertiesOfGroup(labelGroup, vertex);

      } else {
        vertexIsInALabelGroup = vertex.getLabel().equals(labelGroup.getGroupingLabel()) &&
          hasVertexAllPropertiesOfGroup(labelGroup, vertex);
      }
    }
    return vertexIsInALabelGroup;
  }
}
