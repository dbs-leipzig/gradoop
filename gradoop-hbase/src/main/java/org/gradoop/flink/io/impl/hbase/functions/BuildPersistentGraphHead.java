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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

/**
 * Creates persistent graph data from graph data and vertex/edge identifiers.
 *
 * @param <G> EPGM graph head type
 */
public class BuildPersistentGraphHead<G extends EPGMGraphHead>
  implements JoinFunction
  <Tuple3<GradoopId, GradoopIdSet, GradoopIdSet>, G, PersistentGraphHead> {

  /**
   * Persistent graph data factory.
   */
  private PersistentGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates join function.
   *
   * @param graphHeadFactory persistent graph data factory
   */
  public BuildPersistentGraphHead(
    PersistentGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistentGraphHead join(
    Tuple3<GradoopId, GradoopIdSet, GradoopIdSet> longSetSetTuple3, G graphHead)
      throws Exception {
    return graphHeadFactory.createGraphHead(graphHead, longSetSetTuple3.f1,
      longSetSetTuple3.f2);
  }
}
