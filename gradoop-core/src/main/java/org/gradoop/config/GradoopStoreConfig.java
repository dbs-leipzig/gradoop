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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.config;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.api.VertexHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic configuration for Gradoop Stores
 *
 * @param <G>   EPGM graph head type
 * @param <V>   EPGM vertex type
 * @param <E>   EPGM edge type
 * @param <PG>  persistent graph head type
 * @param <PV>  persistent vertex type
 * @param <PE>  persistent edge type
 */
public abstract class GradoopStoreConfig<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  PG extends PersistentGraphHead,
  PV extends PersistentVertex<E>,
  PE extends PersistentEdge<V>>
  extends GradoopConfig<G, V, E> {

  /**
   * Graph head handler.
   */
  private final PersistentGraphHeadFactory<G, PG> persistentGraphHeadFactory;
  /**
   * Vertex handler.
   */
  private final PersistentVertexFactory<V, E, PV> persistentVertexFactory;
  /**
   * Edge handler.
   */
  private final PersistentEdgeFactory<V, E, PE> persistentEdgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param persistentGraphHeadFactory  persistent graph head factory
   * @param persistentVertexFactory     persistent vertex factory
   * @param persistentEdgeFactory       persistent edge factory
   */
  protected GradoopStoreConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler,
    PersistentGraphHeadFactory<G, PG> persistentGraphHeadFactory,
    PersistentVertexFactory<V, E, PV> persistentVertexFactory,
    PersistentEdgeFactory<V, E, PE> persistentEdgeFactory) {
    super(graphHeadHandler, vertexHandler, edgeHandler);

    this.persistentGraphHeadFactory =
      checkNotNull(persistentGraphHeadFactory,
        "PersistentGraphHeadFactory was null");
    this.persistentVertexFactory =
      checkNotNull(persistentVertexFactory,
        "PersistentVertexFactory was null");
    this.persistentEdgeFactory =
      checkNotNull(persistentEdgeFactory,
        "PersistentEdgeFactory was null");
  }

  public PersistentGraphHeadFactory<G, PG> getPersistentGraphHeadFactory() {
    return persistentGraphHeadFactory;
  }

  public PersistentVertexFactory<V, E, PV> getPersistentVertexFactory() {
    return persistentVertexFactory;
  }

  public PersistentEdgeFactory<V, E, PE> getPersistentEdgeFactory() {
    return persistentEdgeFactory;
  }
}
