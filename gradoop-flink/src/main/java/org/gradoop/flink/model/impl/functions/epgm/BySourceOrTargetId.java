/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;

import java.util.Objects;

/**
 * Filters edges having the specified vertex id as source or target.
 *
 * @param <E> the edge type
 */
public class BySourceOrTargetId<E extends Edge> implements CombinableFilter<E> {
  /**
   * Vertex id to filter on
   */
  private final GradoopId vertexId;

  /**
   * Creates a new filter instance.
   *
   * @param vertexId vertex id to filter on
   */
  public BySourceOrTargetId(GradoopId vertexId) {
    this.vertexId = Objects.requireNonNull(vertexId);
  }

  @Override
  public boolean filter(E e) throws Exception {
    return e.getSourceId().equals(vertexId) || e.getTargetId().equals(vertexId);
  }
}
