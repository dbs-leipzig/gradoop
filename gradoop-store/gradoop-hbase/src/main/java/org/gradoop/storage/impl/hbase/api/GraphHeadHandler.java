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
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.common.predicate.query.ElementQuery;

/**
 * This class is responsible for reading and writing EPGM graph heads from
 * and to HBase.
 */
public interface GraphHeadHandler extends ElementHandler {

  /**
   * Adds all graph information to the given {@link Put} and returns it.
   *
   * @param put       put to add graph data to
   * @param graphData graph data
   * @return put with graph data
   */
  Put writeGraphHead(final Put put, final EPGMGraphHead graphData);

  /**
   * Reads the graph data from the given result.
   *
   * @param res HBase row
   * @return graph entity
   */
  GraphHead readGraphHead(final Result res);

  /**
   * Applies the given ElementQuery to the handler.
   *
   * @param query the element query to apply
   * @return the GraphHeadHandler instance with the query applied
   */
  GraphHeadHandler applyQuery(ElementQuery<HBaseElementFilter<GraphHead>> query);

  /**
   * Returns the element query or {@code null}, if no query was applied before.
   *
   * @return the element query or {@code null}, if no query was applied before
   */
  ElementQuery<HBaseElementFilter<GraphHead>> getQuery();
}
