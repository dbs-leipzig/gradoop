/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.io.Serializable;

/**
 * Base interface for creating persistent graph data from transient graph data.
 *
 * @param <G> EPGM graph head type
 */
public interface PersistentGraphHeadFactory<G extends EPGMGraphHead>
  extends Serializable {

  /**
   * Creates graph data based on the given parameters.
   *
   * @param inputGraphData input graph data
   * @param vertices       vertices contained in that graph
   * @param edges          edges contained in that graph
   * @return graph data
   */
  PersistentGraphHead createGraphHead(G inputGraphData,
    GradoopIdSet vertices, GradoopIdSet edges);
}
