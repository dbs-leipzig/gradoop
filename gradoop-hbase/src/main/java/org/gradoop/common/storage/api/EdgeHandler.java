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
package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;

import java.io.IOException;

/**
 * Responsible for reading and writing edge data from and to HBase.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface EdgeHandler<E extends EPGMEdge, V extends EPGMVertex>
  extends GraphElementHandler {

  /**
   * Adds the source vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeSource(final Put put, final V vertexData) throws IOException;

  /**
   * Reads the source vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return source vertex identifier
   */
  GradoopId readSourceId(final Result res) throws IOException;

  /**
   * Adds the target vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeTarget(final Put put, final V vertexData) throws IOException;

  /**
   * Reads the target vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return target vertex identifier
   */
  GradoopId readTargetId(final Result res) throws IOException;

  /**
   * Writes the complete edge data to the given {@link Put} and returns it.
   *
   * @param put      {@link} Put to add edge to
   * @param edgeData edge data to be written
   * @return put with edge data
   */
  Put writeEdge(final Put put, final PersistentEdge<V> edgeData) throws
    IOException;

  /**
   * Reads the edge data from the given {@link Result}.
   *
   * @param res HBase row
   * @return edge data contained in the given result
   */
  E readEdge(final Result res);

  /**
   * Returns the edge data factory used by this handler.
   *
   * @return edge data factory
   */
  EPGMEdgeFactory<E> getEdgeFactory();
}
