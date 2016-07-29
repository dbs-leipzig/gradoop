/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.impl.hbase;

import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.storage.impl.hbase.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Base class for HBase data source and sink.
 *
 * @param <G>  EPGM graph head type
 * @param <V>  EPGM vertex type
 * @param <E>  EPGM edge type
 */
abstract class HBaseBase
  <G extends GraphHead, V extends Vertex, E extends Edge> {
  /**
   * HBase Store implementation
   */
  private final HBaseEPGMStore<G, V, E> epgmStore;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Creates a new HBase data source/sink.
   *
   * @param epgmStore store implementation
   * @param config    Gradoop Flink configuration
   */
  HBaseBase(HBaseEPGMStore<G, V, E> epgmStore,
    GradoopFlinkConfig<G, V, E> config) {
    this.epgmStore  = epgmStore;
    this.config     = config;
  }

  HBaseEPGMStore<G, V, E> getStore() {
    return epgmStore;
  }

  GradoopFlinkConfig<G, V, E> getFlinkConfig() {
    return config;
  }

  GradoopHBaseConfig<G, V, E> getHBaseConfig() {
    return epgmStore.getConfig();
  }
}
