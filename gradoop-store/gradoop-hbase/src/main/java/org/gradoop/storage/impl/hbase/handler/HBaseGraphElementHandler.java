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
package org.gradoop.storage.impl.hbase.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.storage.impl.hbase.api.GraphElementHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;

/**
 * Handler class for entities that are contained in logical graphs (i.e.,
 * vertex and edge data).
 */
public abstract class HBaseGraphElementHandler extends
  HBaseElementHandler implements GraphElementHandler {
  /**
   * Byte representation of the graphs column identifier.
   */
  private static final byte[] COL_GRAPHS_BYTES =
    Bytes.toBytes(HBaseConstants.COL_GRAPHS);

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeGraphIds(Put put, EPGMGraphElement graphElement) {
    if (graphElement.getGraphCount() > 0) {
      put = put.addColumn(
        CF_META_BYTES,
        COL_GRAPHS_BYTES,
        graphElement.getGraphIds().toByteArray()
      );
    }

    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdSet readGraphIds(Result res) {
    byte[] graphBytes = res.getValue(CF_META_BYTES, COL_GRAPHS_BYTES);

    GradoopIdSet graphIds;

    if (graphBytes != null) {
      graphIds = GradoopIdSet.fromByteArray(graphBytes);
    } else {
      graphIds = new GradoopIdSet();
    }

    return graphIds;
  }
}
