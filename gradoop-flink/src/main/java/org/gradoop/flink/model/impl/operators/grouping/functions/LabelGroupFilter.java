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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

/**
 * Function to check whether a vertex is a member of a {@link LabelGroup}.
 *
 * @param <V> The vertex type.
 */
public abstract class LabelGroupFilter<V extends EPGMVertex> implements FilterFunction<V> {

  /**
   * Check whether a vertex exhibits all properties of a {@link LabelGroup}.
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
