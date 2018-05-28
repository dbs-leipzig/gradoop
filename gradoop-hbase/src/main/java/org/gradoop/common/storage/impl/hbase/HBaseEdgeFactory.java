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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentEdgeFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent edge representations.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseEdgeFactory<E extends EPGMEdge, V extends EPGMVertex>
  implements PersistentEdgeFactory<E, V> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseEdge<E, V> createEdge(E inputEdge, V sourceVertex, V
    targetVertex) {
    checkNotNull(inputEdge, "EPGMEdge was null");
    checkNotNull(sourceVertex, "Source vertex was null");
    checkNotNull(targetVertex, "Target vertex was null");
    return new HBaseEdge<>(inputEdge, sourceVertex, targetVertex);
  }
}
