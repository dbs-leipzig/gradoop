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
package org.gradoop.common.config;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdgeFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertexFactory;

/**
 * Basic Gradoop Configuration.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopConfig
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Knows how to create {@link GraphHead}
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Knows how to create {@link Vertex}
   */
  private final VertexFactory vertexFactory;

  /**
   *  Knows how to create {@link Edge}
   */
  private final EdgeFactory edgeFactory;

  /**
   * Knows how to create {@link org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead}
   */
  private TemporalGraphHeadFactory temporalGraphHeadFactory;

  /**
   * Knows how to create {@link org.gradoop.common.model.impl.pojo.temporal.TemporalVertex}
   */
  private TemporalVertexFactory temporalVertexFactory;

  /**
   * Knows how to create {@link org.gradoop.common.model.impl.pojo.temporal.TemporalEdge}
   */
  private TemporalEdgeFactory temporalEdgeFactory;

  /**
   * Creates a new Configuration.
   */
  @SuppressWarnings("unchecked")
  protected GradoopConfig() {
    this.graphHeadFactory = new GraphHeadFactory();
    this.vertexFactory = new VertexFactory();
    this.edgeFactory = new EdgeFactory();
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads.
   *
   * @return Default Gradoop configuration.
   */
  public static GradoopConfig<GraphHead, Vertex, Edge> getDefaultConfig() {
    return new GradoopConfig<>();
  }

  public GraphHeadFactory getGraphHeadFactory() {
    return graphHeadFactory;
  }

  public VertexFactory getVertexFactory() {
    return vertexFactory;
  }

  public EdgeFactory getEdgeFactory() {
    return edgeFactory;
  }

  /**
   * Returns a factory that is responsible for creating a
   * {@link org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead} instance.
   *
   * @return the temporal graph head factory
   */
  public TemporalGraphHeadFactory getTemporalGraphHeadFactory() {
    if (temporalGraphHeadFactory == null) {
      temporalGraphHeadFactory = new TemporalGraphHeadFactory();
    }
    return temporalGraphHeadFactory;
  }

  /**
   * Returns a factory that is responsible for creating a
   * {@link org.gradoop.common.model.impl.pojo.temporal.TemporalVertex} instance.
   *
   * @return the temporal vertex factory
   */
  public TemporalVertexFactory getTemporalVertexFactory() {
    if (temporalVertexFactory == null) {
      temporalVertexFactory = new TemporalVertexFactory();
    }
    return temporalVertexFactory;
  }

  /**
   * Returns a factory that is responsible for creating a
   * {@link org.gradoop.common.model.impl.pojo.temporal.TemporalEdge} instance.
   *
   * @return the temporal edge factory
   */
  public TemporalEdgeFactory getTemporalEdgeFactory() {
    if (temporalEdgeFactory == null) {
      temporalEdgeFactory = new TemporalEdgeFactory();
    }
    return temporalEdgeFactory;
  }
}
