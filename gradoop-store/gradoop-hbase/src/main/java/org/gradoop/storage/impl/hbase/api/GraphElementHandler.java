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
package org.gradoop.storage.impl.hbase.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;

import java.io.IOException;

/**
 * Responsible for reading and writing graph element entity data from and to
 * HBase. These are usually vertex and edge data objects.
 */
public interface GraphElementHandler extends ElementHandler {
  /**
   * Adds the given graph identifiers to the given {@link Put} and returns it.
   *
   * @param put          {@link Put} to add graph identifiers to
   * @param graphElement graph element
   * @return put with graph identifiers
   */
  Put writeGraphIds(
    final Put put,
    final EPGMGraphElement graphElement
  ) throws
    IOException;

  /**
   * Reads the graph identifiers from the given {@link Result}.
   *
   * @param res HBase row
   * @return graphs identifiers
   */
  GradoopIdSet readGraphIds(final Result res);
}
