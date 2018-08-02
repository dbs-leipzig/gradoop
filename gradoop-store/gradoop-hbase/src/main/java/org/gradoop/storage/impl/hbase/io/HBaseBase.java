/*
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
package org.gradoop.storage.impl.hbase.io;

import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;

import javax.annotation.Nonnull;

/**
 * Base class for HBase data source and sink.
 */
abstract class HBaseBase {
  /**
   * HBase Store implementation
   */
  private final HBaseEPGMStore epgmStore;

  /**
   * Gradoop flink configuration
   */
  private final GradoopFlinkConfig flinkConfig;

  /**
   * Creates a new HBase data source/sink.
   *
   * @param flinkConfig gradoop flink execute config
   * @param epgmStore store implementation
   */
  HBaseBase(
    @Nonnull HBaseEPGMStore epgmStore,
    @Nonnull GradoopFlinkConfig flinkConfig
  ) {
    this.epgmStore = epgmStore;
    this.flinkConfig = flinkConfig;
  }

  HBaseEPGMStore getStore() {
    return epgmStore;
  }

  GradoopHBaseConfig getHBaseConfig() {
    return epgmStore.getConfig();
  }

  GradoopFlinkConfig getFlinkConfig() {
    return flinkConfig;
  }

}
