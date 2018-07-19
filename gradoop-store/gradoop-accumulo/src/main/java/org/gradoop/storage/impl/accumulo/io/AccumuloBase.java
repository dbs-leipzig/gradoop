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
package org.gradoop.storage.impl.accumulo.io;

import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;

/**
 * Base class for Accumulo data source and sink.
 */
abstract class AccumuloBase {

  /**
   * Accumulo Store implementation
   */
  private final AccumuloEPGMStore epgmStore;

  /**
   * Creates a new Accumulo data source/sink.
   *
   * @param epgmStore store implementation
   */
  AccumuloBase(AccumuloEPGMStore epgmStore) {
    this.epgmStore = epgmStore;
  }

  AccumuloEPGMStore getStore() {
    return epgmStore;
  }

  GradoopAccumuloConfig getAccumuloConfig() {
    return epgmStore.getConfig();
  }

}
