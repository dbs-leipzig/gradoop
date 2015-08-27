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

package org.gradoop.storage;

import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;

import java.io.IOException;
import java.util.Iterator;

/**
 * The EPGM store is responsible for writing and reading graph, vertex and
 * edge data.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public interface EPGMStore<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> {
  /**
   * Returns the vertex data handler used by this store.
   *
   * @return vertex data handler
   */
  VertexDataHandler<VD, ED> getVertexDataHandler();

  /**
   * Returns the edge data handler used by this store.
   *
   * @return edge data handler
   */
  EdgeDataHandler<ED, VD> getEdgeDataHandler();

  /**
   * Returns the graph data handler used by this store.
   *
   * @return graph data handler
   */
  GraphDataHandler<GD> getGraphDataHandler();

  /**
   * Returns the HBase table name where vertex data is stored.
   *
   * @return vertex data table name
   */
  String getVertexDataTableName();

  /**
   * Returns the HBase table name where edge data is stored.
   *
   * @return edge data table name
   */
  String getEdgeDataTableName();

  /**
   * Returns the HBase table name where graph data is stored.
   *
   * @return graph data table name
   */
  String getGraphDataTableName();

  /**
   * Writes the given graph data into the graph store.
   *
   * @param graphData graph data to write
   */
  void writeGraphData(final PersistentGraphData graphData);

  /**
   * Writes the given vertex data into the graph store.
   *
   * @param vertexData vertex data to write
   */
  void writeVertexData(final PersistentVertexData<ED> vertexData);

  /**
   * Writes the given edge data into the graph store.
   *
   * @param edgeData edge data to write
   */
  void writeEdgeData(final PersistentEdgeData<VD> edgeData);

  /**
   * Reads a graph data entity from the EPGM store using the given graph
   * identifier. If {@code graphId} does not exist, {@code null} is returned.
   *
   * @param graphId graph identifier
   * @return graph data entity or {@code null} if there is no entity with the
   * given {@code graphId}
   */
  GD readGraphData(final Long graphId);

  /**
   * Reads a vertex data entity from the EPGM store using the given vertex
   * identifier. If {@code vertexId} does not exist, {@code null} is returned.
   *
   * @param vertexId vertex identifier
   * @return vertex data entity or {@code null} if there is no entity with the
   * given {@code vertexId}
   */
  VD readVertexData(final Long vertexId);

  /**
   * Reads an edge data entity from the EPGM store using the given edge
   * identifier. If {@code edgeId} does not exist, {@code null} is returned.
   *
   * @param edgeId edge identifier
   * @return edge data entity or {@code null} if there is no entity with the
   * given {@code edgeId}
   */
  ED readEdgeData(final Long edgeId);

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<VD> getVertexSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<VD> getVertexSpace(int cacheSize) throws InterruptedException,
    IOException, ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<ED> getEdgeSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @param cacheSize cache size for HBase scan
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<ED> getEdgeSpace(int cacheSize) throws InterruptedException,
    IOException, ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<GD> getGraphSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<GD> getGraphSpace(int cacheSize) throws InterruptedException,
    IOException, ClassNotFoundException;

  /**
   * Setting this value to true, forces the store implementation to flush the
   * write buffers after every write.
   *
   * @param autoFlush true to enable auto flush, false to disable
   */
  void setAutoFlush(boolean autoFlush);

  /**
   * Flushes all buffered writes to the store.
   */
  void flush();

  /**
   * Closes the graph store and flushes all writes.
   */
  void close();


}
