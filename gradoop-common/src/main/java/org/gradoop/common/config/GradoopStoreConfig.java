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

package org.gradoop.common.config;

import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentEdgeFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.common.storage.api.VertexHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic configuration for Gradoop Stores
 */
public abstract class GradoopStoreConfig extends GradoopConfig {

  /**
   * Graph head handler.
   */
  private final PersistentGraphHeadFactory persistentGraphHeadFactory;
  /**
   * Vertex handler.
   */
  private final PersistentVertexFactory persistentVertexFactory;
  /**
   * Edge handler.
   */
  private final PersistentEdgeFactory persistentEdgeFactory;

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
    GraphHeadHandler graphHeadHandler,
    VertexHandler vertexHandler,
    EdgeHandler edgeHandler,
    PersistentGraphHeadFactory persistentGraphHeadFactory,
    PersistentVertexFactory persistentVertexFactory,
    PersistentEdgeFactory persistentEdgeFactory) {
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

  public PersistentGraphHeadFactory getPersistentGraphHeadFactory() {
    return persistentGraphHeadFactory;
  }

  public PersistentVertexFactory getPersistentVertexFactory() {
    return persistentVertexFactory;
  }

  public PersistentEdgeFactory getPersistentEdgeFactory() {
    return persistentEdgeFactory;
  }
}
