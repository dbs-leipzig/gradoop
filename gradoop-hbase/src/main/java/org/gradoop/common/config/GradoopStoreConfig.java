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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.PersistentEdgeFactory;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Basic configuration for Gradoop Stores
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */	
public abstract class GradoopStoreConfig<G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GradoopFlinkConfig {

  /**
   * Graph head handler.
   */
  private final PersistentGraphHeadFactory<G> persistentGraphHeadFactory;
  /**
   * EPGMVertex handler.
   */
  private final PersistentVertexFactory<V, E> persistentVertexFactory;
  /**
   * Edge handler.
   */
  private final PersistentEdgeFactory<E, V> persistentEdgeFactory;

  /**
   * Creates a new Configuration.
   *
   * @param persistentGraphHeadFactory  persistent graph head factory
   * @param persistentVertexFactory     persistent vertex factory
   * @param persistentEdgeFactory       persistent edge factory
   * @param env                         Flink {@link ExecutionEnvironment}
   */
  protected GradoopStoreConfig(
    PersistentGraphHeadFactory<G> persistentGraphHeadFactory,
    PersistentVertexFactory<V, E> persistentVertexFactory,
    PersistentEdgeFactory<E, V> persistentEdgeFactory,
    ExecutionEnvironment env) {
    super(env);

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

  public PersistentGraphHeadFactory<G> getPersistentGraphHeadFactory() {
    return persistentGraphHeadFactory;
  }

  public PersistentVertexFactory<V, E> getPersistentVertexFactory() {
    return persistentVertexFactory;
  }

  public PersistentEdgeFactory<E, V> getPersistentEdgeFactory() {
    return persistentEdgeFactory;
  }
}
