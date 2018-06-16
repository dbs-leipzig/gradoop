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

import edu.umd.cs.findbugs.annotations.NonNull;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.iterator.ClosableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * definition of graph store output
 *
 * @param <GOutput> graph head(output)
 * @param <VOutput> graph vertex(output)
 * @param <EOutput> graph edge(output)
 */
public interface EPGMGraphOutput<
  GOutput extends EPGMGraphHead,
  VOutput extends EPGMVertex,
  EOutput extends EPGMEdge> {

  /**
   * default cache size, if client cache size is not provided
   */
  int DEFAULT_CACHE_SIZE = 5000;

  /**
   * Reads a graph data entity from the EPGM store using the given graph
   * identifier. If {@code graphId} does not exist, {@code null} is returned.
   *
   * @param graphId graph identifier
   * @return graph data entity or {@code null} if there is no entity with the
   *         given {@code graphId}
   * @throws IOException if error occur on IO error (timeout, conn disconnected)
   */
  @Nullable
  GOutput readGraph(final GradoopId graphId) throws IOException;

  /**
   * Reads all graphs from the EPGM store.
   *
   * @return all graphs
   * @throws IOException unexpected IO error (timeout, conn disconnected)
   */
  @NonNull
  default ClosableIterator<GOutput> getGraphSpace() throws IOException {
    return getGraphSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all graphs from the EPGM store.
   *
   * @param cacheSize cache size for client scan
   * @return all graphs
   */
  @Nonnull
  ClosableIterator<GOutput> getGraphSpace(int cacheSize) throws IOException;

  /**
   * Reads a vertex data entity from the EPGM store using the given vertex
   * identifier. If {@code vertexId} does not exist, {@code null} is returned.
   *
   * @param vertexId vertex identifier
   * @return vertex data entity or {@code null} if there is no entity with the
   * given {@code vertexId}
   */
  @Nullable
  VOutput readVertex(final GradoopId vertexId) throws IOException;

  /**
   * Reads all vertices from the EPGM store.
   *
   * @return all vertices
   */
  @Nonnull
  default ClosableIterator<VOutput> getVertexSpace() throws IOException {
    return getVertexSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all vertices from the EPGM store.
   *
   * @param cacheSize cache size for client scan
   * @return all vertices
   */
  @Nonnull
  ClosableIterator<VOutput> getVertexSpace(int cacheSize) throws IOException;

  /**
   * Reads an edge data entity from the EPGM store using the given edge
   * identifier. If {@code edgeId} does not exist, {@code null} is returned.
   *
   * @param edgeId edge identifier
   * @return edge data entity or {@code null} if there is no entity with the
   * given {@code edgeId}
   */
  @Nullable
  EOutput readEdge(final GradoopId edgeId) throws IOException;

  /**
   * Reads all edges from the EPGM store.
   *
   * @return all edges
   */
  @Nonnull
  default ClosableIterator<EOutput> getEdgeSpace() throws IOException {
    return getEdgeSpace(DEFAULT_CACHE_SIZE);
  }

  /**
   * Reads all edges from the EPGM store.
   *
   * @param cacheSize cache size for client scan
   * @return all edges
   */
  @Nonnull
  ClosableIterator<EOutput> getEdgeSpace(int cacheSize) throws IOException;

}
