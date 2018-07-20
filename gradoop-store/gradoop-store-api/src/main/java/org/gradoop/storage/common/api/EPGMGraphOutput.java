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
package org.gradoop.storage.common.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.iterator.ClosableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Definition of graph store output.
 * A graph input instance provide a set of query methods for EPGM Elements
 * both id query and traversal query are supported
 */
public interface EPGMGraphOutput {

  /**
   * Default cache size, if client cache size is not provided
   */
  int DEFAULT_CACHE_SIZE = 5000;

  /**
   * Reads a graph data entity from the EPGMGraphOutput using the given graph
   * identifier. If {@code graphId} does not exist, {@code null} is returned.
   *
   * @param graphId   graph identifier
   * @return graph data entity or {@code null}
   *         if there is no entity with the given {@code graphId}
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nullable
  GraphHead readGraph(@Nonnull GradoopId graphId) throws IOException;

  /**
   * Reads all graphs from the EPGMGraphOutput.
   *
   * @return all graphs
   * @throws IOException unexpected IO error (timeout, conn disconnected)
   */
  @NonNull
  default ClosableIterator<GraphHead> getGraphSpace() throws IOException {
    return getGraphSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all graphs from the EPGMGraphOutput.
   *
   * @param cacheSize cache size for client scan
   * @return all graphs
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nonnull
  ClosableIterator<GraphHead> getGraphSpace(int cacheSize) throws IOException;

  /**
   * Reads a vertex data entity from the EPGMGraphOutput using the given vertex
   * identifier. If {@code vertexId} does not exist, {@code null} is returned.
   *
   * @param vertexId vertex identifier
   * @return vertex data entity or {@code null} if there is no entity with the
   * given {@code vertexId}
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nullable
  Vertex readVertex(@Nonnull GradoopId vertexId) throws IOException;

  /**
   * Reads all vertices from the EPGMGraphOutput.
   *
   * @return all vertices
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nonnull
  default ClosableIterator<Vertex> getVertexSpace() throws IOException {
    return getVertexSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all vertices from the EPGMGraphOutput.
   *
   * @param cacheSize cache size for client scan
   * @return all vertices
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nonnull
  ClosableIterator<Vertex> getVertexSpace(int cacheSize) throws IOException;

  /**
   * Reads an edge data entity from the EPGMGraphOutput using the given edge
   * identifier. If {@code edgeId} does not exist, {@code null} is returned.
   *
   * @param edgeId edge identifier
   * @return edge data entity or {@code null} if there is no entity with the
   * given {@code edgeId}
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nullable
  Edge readEdge(@Nonnull GradoopId edgeId) throws IOException;

  /**
   * Reads all edges from the EPGMGraphOutput.
   *
   * @return all edges
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nonnull
  default ClosableIterator<Edge> getEdgeSpace() throws IOException {
    return getEdgeSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all edges from the EPGMGraphOutput.
   *
   * @param cacheSize cache size for client scan
   * @return all edges
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nonnull
  ClosableIterator<Edge> getEdgeSpace(int cacheSize) throws IOException;

}
