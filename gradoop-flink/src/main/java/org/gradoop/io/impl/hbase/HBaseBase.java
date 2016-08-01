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

import org.gradoop.storage.impl.hbase.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Base class for HBase data source and sink.
 */
abstract class HBaseBase {
  /**
   * HBase Store implementation
   */
  private final HBaseEPGMStore epgmStore;
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
  HBaseBase(HBaseEPGMStore epgmStore,
    GradoopFlinkConfig config) {
    this.epgmStore  = epgmStore;
    this.config     = config;
  }

  HBaseEPGMStore getStore() {
    return epgmStore;
  }

  GradoopFlinkConfig getFlinkConfig() {
    return config;
  }

  GradoopHBaseConfig getHBaseConfig() {
    return epgmStore.getConfig();
  }
}
