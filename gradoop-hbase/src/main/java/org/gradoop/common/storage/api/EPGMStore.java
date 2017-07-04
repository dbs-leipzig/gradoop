/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.gradoop.common.config.GradoopStoreConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;

import java.io.IOException;
import java.util.Iterator;

/**
 * The EPGM store is responsible for writing and reading graph heads, vertices
 * and edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public interface EPGMStore
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * Returns the Gradoop configuration associated with that EPGM Store,
   *
   * @return Gradoop Configuration
   */
  GradoopStoreConfig getConfig();

  /**
   * Returns the HBase table name where vertex data is stored.
   *
   * @return vertex data table name
   */
  String getVertexTableName();

  /**
   * Returns the HBase table name where edge data is stored.
   *
   * @return edge data table name
   */
  String getEdgeTableName();

  /**
   * Returns the HBase table name where graph data is stored.
   *
   * @return graph data table name
   */
  String getGraphHeadName();

  /**
   * Writes the given graph data into the graph store.
   *
   * @param graphData graph data to write
   */
  void writeGraphHead(final PersistentGraphHead graphData);

  /**
   * Writes the given vertex data into the graph store.
   *
   * @param vertexData vertex data to write
   */
  void writeVertex(final PersistentVertex<E> vertexData);

  /**
   * Writes the given edge data into the graph store.
   *
   * @param edgeData edge data to write
   */
  void writeEdge(final PersistentEdge<V> edgeData);

  /**
   * Reads a graph data entity from the EPGM store using the given graph
   * identifier. If {@code graphId} does not exist, {@code null} is returned.
   *
   * @param graphId graph identifier
   * @return graph data entity or {@code null} if there is no entity with the
   * given {@code graphId}
   */
  G readGraph(final GradoopId graphId);

  /**
   * Reads a vertex data entity from the EPGM store using the given vertex
   * identifier. If {@code vertexId} does not exist, {@code null} is returned.
   *
   * @param vertexId vertex identifier
   * @return vertex data entity or {@code null} if there is no entity with the
   * given {@code vertexId}
   */
  V readVertex(final GradoopId vertexId);

  /**
   * Reads an edge data entity from the EPGM store using the given edge
   * identifier. If {@code edgeId} does not exist, {@code null} is returned.
   *
   * @param edgeId edge identifier
   * @return edge data entity or {@code null} if there is no entity with the
   * given {@code edgeId}
   */
  E readEdge(final GradoopId edgeId);

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<V> getVertexSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all vertices from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all vertices or {@code null} if EPGM store is empty
   */
  Iterator<V> getVertexSpace(int cacheSize) throws InterruptedException,
    IOException, ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<E> getEdgeSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all edges from the EPGM store. If no edges exist, {@code null} is
   * returned,
   *
   * @param cacheSize cache size for HBase scan
   * @return all edges or {@code null} if no edges exist
   */
  Iterator<E> getEdgeSpace(int cacheSize) throws InterruptedException,
    IOException, ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<G> getGraphSpace() throws InterruptedException, IOException,
    ClassNotFoundException;

  /**
   * Reads all graphs from the EPGM store. If EPGM store is empty, {@code
   * null} is returned.
   *
   * @param cacheSize cache size for HBase scan
   * @return all graphs or {@code null} if EPGM store is empty
   */
  Iterator<G> getGraphSpace(int cacheSize) throws InterruptedException,
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
