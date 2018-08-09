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
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

/**
 * Responsible for reading and writing edge data from and to HBase.
 */
public interface EdgeHandler extends GraphElementHandler {

  /**
   * Adds the source vertex data to the given {@link Put} and returns it.
   *
   * @param put HBase {@link Put}
   * @param sourceId source vertex id
   * @return put with vertex data
   */
  Put writeSource(final Put put, final GradoopId sourceId);

  /**
   * Reads the source vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return source vertex identifier
   */
  GradoopId readSourceId(final Result res);

  /**
   * Adds the target vertex data to the given {@link Put} and returns it.
   *
   * @param put HBase {@link Put}
   * @param targetId target vertex id
   * @return put with vertex data
   */
  Put writeTarget(final Put put, final GradoopId targetId);

  /**
   * Reads the target vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return target vertex identifier
   */
  GradoopId readTargetId(final Result res);

  /**
   * Writes the complete edge data to the given {@link Put} and returns it.
   *
   * @param put      {@link} Put to add edge to
   * @param edgeData edge data to be written
   * @return put with edge data
   */
  Put writeEdge(final Put put, final EPGMEdge edgeData);

  /**
   * Reads the edge data from the given {@link Result}.
   *
   * @param res HBase row
   * @return edge data contained in the given result
   */
  Edge readEdge(final Result res);

  /**
   * Applies the given ElementQuery to the handler.
   *
   * @param query the element query to apply
   * @return the EdgeHandler instance with the query applied
   */
  EdgeHandler applyQuery(ElementQuery<HBaseElementFilter<Edge>> query);

  /**
   * Returns the element query or {@code null}, if no query was applied before.
   *
   * @return the element query or {@code null}, if no query was applied before
   */
  ElementQuery<HBaseElementFilter<Edge>> getQuery();
}
