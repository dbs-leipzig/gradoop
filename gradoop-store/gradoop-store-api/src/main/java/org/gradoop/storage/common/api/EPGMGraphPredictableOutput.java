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

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;
import org.gradoop.storage.common.predicate.query.ElementQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Definition of predictable graph store output.
 * A graph input instance provide a set of predictable query methods for EPGM Elements
 * Only elements fulfill predicate will be return by table server side
 *
 * @param <GFilter>   graph element filter type
 * @param <VFilter>   vertex element filter type
 * @param <EFilter>   edge element filter type
 * @see ElementQuery  EPGM element query formula
 */
public interface EPGMGraphPredictableOutput<
  GFilter extends ElementFilter,
  VFilter extends ElementFilter,
  EFilter extends ElementFilter> extends EPGMGraphOutput {

  @Nonnull
  @Override
  default ClosableIterator<GraphHead> getGraphSpace(int cacheSize) throws IOException {
    return getGraphSpace(null, cacheSize);
  }

  @Nonnull
  @Override
  default ClosableIterator<Vertex> getVertexSpace(int cacheSize) throws IOException {
    return getVertexSpace(null, cacheSize);
  }

  @Nonnull
  @Override
  default ClosableIterator<Edge> getEdgeSpace(int cacheSize) throws IOException {
    return getEdgeSpace(null, cacheSize);
  }

  /**
   * Read graph elements from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @return Graph Heads
   */
  @Nonnull
  default ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<GFilter> query
  ) throws IOException {
    return getGraphSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * Read graph elements from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @param cacheSize client result cache size
   * @return Graph Heads
   */
  @Nonnull
  ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<GFilter> query,
    int cacheSize
  ) throws IOException;

  /**
   * Read vertices from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @return Vertices
   */
  @Nonnull
  default ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<VFilter> query
  ) throws IOException {
    return getVertexSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * Read vertices from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @param cacheSize result cache size
   * @return Vertices
   */
  @Nonnull
  ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<VFilter> query,
    int cacheSize
  ) throws IOException;

  /**
   * Read edges from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @return edges
   */
  @Nonnull
  default ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<EFilter> query
  ) throws IOException {
    return getEdgeSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * Read edges from the EPGMGraphOutput by element query
   *
   * @param query element query predicate
   * @param cacheSize result cache size
   * @return edges
   */
  @Nonnull
  ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<EFilter> query,
    int cacheSize
  ) throws IOException;

}
