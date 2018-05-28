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
package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentEdgeFactory;

/**
 * Creates persistent edge data objects from edge data and source/target
 * vertex data.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class BuildPersistentEdge<V extends EPGMVertex, E extends EPGMEdge>
  implements JoinFunction<Tuple2<V, E>, V, PersistentEdge<V>> {

  /**
   * Persistent edge data factory.
   */
  private final PersistentEdgeFactory<E, V> edgeFactory;

  /**
   * Creates join function
   *
   * @param edgeFactory persistent edge data factory.
   */
  public BuildPersistentEdge(PersistentEdgeFactory<E, V> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistentEdge<V> join(Tuple2<V, E> sourceVertexAndEdge, V targetVertex) throws Exception {
    return edgeFactory.createEdge(sourceVertexAndEdge.f1, sourceVertexAndEdge.f0, targetVertex);
  }
}
