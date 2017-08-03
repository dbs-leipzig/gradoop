/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Filters edges having the specified source vertex id.
 *
 * @param <E> EPGM edge type
 */
public class BySourceId<E extends Edge> implements FilterFunction<E> {
  /**
   * Vertex id to filter on
   */
  private final GradoopId sourceId;

  /**
   * Constructor
   *
   * @param sourceId vertex id to filter on
   */
  public BySourceId(GradoopId sourceId) {
    this.sourceId = sourceId;
  }

  @Override
  public boolean filter(E e) throws Exception {
    return e.getSourceId().equals(sourceId);
  }
}
