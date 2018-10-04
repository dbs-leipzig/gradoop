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

package org.gradoop.storage.common.io;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Data source with support for id index an incident index.
 * A Source extending this interface is able to:
 *
 *  - seek element by id
 *  - seek edge id by isolated vertex id
 *  - seek vertex id by isolated edge id
 *
 * @param <GFilter> GraphHead element filter
 * @param <VFilter> Vertex element filter
 * @param <EFilter> Edge element filter
 */
public interface IndexedDataSource<
  GFilter extends ElementFilter,
  VFilter extends ElementFilter,
  EFilter extends ElementFilter> extends DataSource {

  /**
   * Return all graph heads from data source
   *
   * @return edge definitions
   * @throws IOException read error
   */
  default DataSet<GraphHead> getGraphHeads() throws IOException {
    return getGraphCollection().getGraphHeads();
  }

  /**
   * Return all vertices from data source
   *
   * @return edge definitions
   * @throws IOException read error
   */
  default DataSet<Vertex> getVertices() throws IOException {
    return getGraphCollection().getVertices();
  }

  /**
   * Return all edges from data source
   *
   * @return edge definitions
   * @throws IOException read error
   */
  default DataSet<Edge> getEdges() throws IOException {
    return getGraphCollection().getEdges();
  }

  /**
   * Return all graph heads that fulfill given predicate
   *
   * @param filter predicate formula
   * @return graph head data set
   */
  @Nonnull
  DataSet<GraphHead> getGraphHeads(@Nonnull GFilter filter) throws IOException;

  /**
   * Return all vertices that fulfill given predicate
   *
   * @param filter predicate formula
   * @return vertex data set
   */
  @Nonnull
  DataSet<Vertex> getVertices(@Nonnull VFilter filter) throws IOException;

  /**
   * Return all edges that fulfill given predicate
   *
   * @param filter predicate formula
   * @return edge data set
   */
  @Nonnull
  DataSet<Edge> getEdges(@Nonnull EFilter filter) throws IOException;

  /**
   * Return all fulfilled graph head in given id range
   *
   * @param graphHeadIds graph head id range
   * @param filter predicate formula, return all if null
   * @return graph head data set
   */
  @Nonnull
  DataSet<GraphHead> getGraphHeads(
    @Nonnull DataSet<GradoopId> graphHeadIds,
    @Nullable GFilter filter
  ) throws IOException;

  /**
   * Return all fulfilled vertex in given id range
   *
   * @param vertexIds vertex id range
   * @param filter predicate formula, return all if null
   * @return vertex data set
   */
  @Nonnull
  DataSet<Vertex> getVertices(
    @Nonnull DataSet<GradoopId> vertexIds,
    @Nullable VFilter filter
  ) throws IOException;

  /**
   * Return all fulfilled edge in given id range
   *
   * @param edgeIds edge id range
   * @param filter predicate formula, return all if null
   * @return edge data set
   */
  @Nonnull
  DataSet<Edge> getEdges(
    @Nonnull DataSet<GradoopId> edgeIds,
    @Nullable EFilter filter
  ) throws IOException;

}
