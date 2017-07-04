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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default factory for creating persistent graph data representation.
 *
 * @param <G> EPGM graph head type
 */
public class HBaseGraphHeadFactory<G extends EPGMGraphHead>
  implements PersistentGraphHeadFactory<G> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public HBaseGraphHead<G> createGraphHead(G inputGraphHead,
    GradoopIdList vertices, GradoopIdList edges) {
    checkNotNull(inputGraphHead, "EPGMGraphHead was null");
    checkNotNull(vertices, "EPGMVertex identifiers were null");
    checkNotNull(edges, "EPGMEdge identifiers were null");
    return new HBaseGraphHead<>(inputGraphHead, vertices, edges);
  }
}
