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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.VertexData;

/**
 * Responsible for reading and writing edge data from and to HBase.
 *
 * @param <ED> edge data type
 * @param <VD> vertex data type
 */
public interface EdgeDataHandler<ED extends EdgeData, VD extends VertexData>
  extends
  GraphElementHandler {

  /**
   * Adds the source vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeSourceVertex(final Put put, final VD vertexData);

  /**
   * Reads the source vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return source vertex identifier
   */
  Long readSourceVertexId(final Result res);

  /**
   * Adds the target vertex data to the given {@link Put} and returns it.
   *
   * @param put        HBase HBase {@link Put}
   * @param vertexData vertex data
   * @return put with vertex data
   */
  Put writeTargetVertex(final Put put, final VD vertexData);

  /**
   * Reads the target vertex identifier from the given {@link Result}.
   *
   * @param res HBase {@link Result}
   * @return target vertex identifier
   */
  Long readTargetVertexId(final Result res);

  /**
   * Writes the complete edge data to the given {@link Put} and returns it.
   *
   * @param put      {@link} Put to add edge to
   * @param edgeData edge data to be written
   * @return put with edge data
   */
  Put writeEdgeData(final Put put, final PersistentEdgeData<VD> edgeData);

  /**
   * Reads the edge data from the given {@link Result}.
   *
   * @param res HBase row
   * @return edge data contained in the given result
   */
  ED readEdgeData(final Result res);

  /**
   * Returns the edge data factory used by this handler.
   *
   * @return edge data factory
   */
  EdgeDataFactory<ED> getEdgeDataFactory();
}
