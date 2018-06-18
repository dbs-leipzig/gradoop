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

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.iterator.ClosableIterator;
import org.gradoop.common.storage.predicate.filter.api.ElementFilter;
import org.gradoop.common.storage.predicate.query.ElementQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * create graph output iterator with external predicate
 *
 * @param <GFilter> graph factory
 * @param <VFilter> vertex factory
 * @param <EFilter> edge factory
 */
public interface EPGMGraphPredictableOutput<
  GFilter extends ElementFilter,
  VFilter extends ElementFilter,
  EFilter extends ElementFilter> extends EPGMGraphOutput {

  @Nonnull
  @Override
  default ClosableIterator<GraphHead> getGraphSpace(int cacheSize) throws IOException {
    return getGraphSpace(null, DEFAULT_CACHE_SIZE);
  }

  @Nonnull
  @Override
  default ClosableIterator<Vertex> getVertexSpace(int cacheSize) throws IOException {
    return getVertexSpace(null, DEFAULT_CACHE_SIZE);
  }

  @Nonnull
  @Override
  default ClosableIterator<Edge> getEdgeSpace(int cacheSize) throws IOException {
    return getEdgeSpace(null, DEFAULT_CACHE_SIZE);
  }

  /**
   * get graphs by filter predicate
   *
   * @param query filter predicate
   * @return edges
   */
  @Nonnull
  default ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<GFilter> query
  ) throws IOException {
    return getGraphSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * get graphs by filter predicate
   *
   * @param query filter predicate
   * @param cacheSize result cache size
   * @return edges
   */
  @Nonnull
  ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<GFilter> query,
    int cacheSize
  ) throws IOException;

  /**
   * get vertices by filter predicate
   *
   * @param query filter predicate
   * @return vertices
   */
  @Nonnull
  default ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<VFilter> query
  ) throws IOException {
    return getVertexSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * get vertices by filter predicate
   *
   * @param query filter predicate
   * @param cacheSize result cache size
   * @return vertices
   */
  @Nonnull
  ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<VFilter> query,
    int cacheSize
  ) throws IOException;

  /**
   * get edges by filter predicate
   *
   * @param query filter predicate
   * @return edges
   */
  @Nonnull
  default ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<EFilter> query
  ) throws IOException {
    return getEdgeSpace(query, DEFAULT_CACHE_SIZE);
  }

  /**
   * get edges by filter predicate
   *
   * @param query filter predicate
   * @param cacheSize result cache size
   * @return edges
   */
  @Nonnull
  ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<EFilter> query,
    int cacheSize
  ) throws IOException;

}
