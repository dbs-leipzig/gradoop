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
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.common.predicate.query.ElementQuery;

/**
 * Responsible for reading and writing vertex data from and to HBase.
 */
public interface VertexHandler extends GraphElementHandler {

  /**
   * Writes the complete vertex data to the given {@link Put} and returns it.
   *
   * @param put        {@link Put} to add vertex to
   * @param vertexData vertex data to be written
   * @return put with vertex data
   */
  Put writeVertex(final Put put, final EPGMVertex vertexData);

  /**
   * Reads the vertex data from the given {@link Result}.
   *
   * @param res HBase row
   * @return vertex data contained in the given result.
   */
  Vertex readVertex(final Result res);

  /**
   * Applies the given ElementQuery to the handler.
   *
   * @param query the element query to apply
   * @return the VertexHandler instance with the query applied
   */
  VertexHandler applyQuery(ElementQuery<HBaseElementFilter<Vertex>> query);

  /**
   * Returns the element query or {@code null}, if no query was applied before.
   *
   * @return the element query or {@code null}, if no query was applied before
   */
  ElementQuery<HBaseElementFilter<Vertex>> getQuery();
}
