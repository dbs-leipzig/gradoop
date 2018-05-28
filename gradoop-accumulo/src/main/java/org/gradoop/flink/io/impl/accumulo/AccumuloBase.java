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

package org.gradoop.flink.io.impl.accumulo;

import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for HBase data source and sink.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
abstract class AccumuloBase<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * HBase Store implementation
   */
  private final AccumuloEPGMStore<G, V, E> epgmStore;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new HBase data source/sink.
   *
   * @param epgmStore store implementation
   * @param config    Gradoop Flink configuration
   */
  AccumuloBase(AccumuloEPGMStore<G, V, E> epgmStore, GradoopFlinkConfig config) {
    this.epgmStore = epgmStore;
    this.config = config;
  }

  AccumuloEPGMStore<G, V, E> getStore() {
    return epgmStore;
  }

  GradoopFlinkConfig getFlinkConfig() {
    return config;
  }

  GradoopAccumuloConfig<G, V, E> getAccumuloConfig() {
    return epgmStore.getConfig();
  }

}
