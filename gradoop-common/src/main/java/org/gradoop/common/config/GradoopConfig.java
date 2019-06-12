/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;

/**
 * Basic Gradoop Configuration.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopConfig
  <G extends GraphHead, V extends Vertex, E extends Edge> {

  /**
   * Knows how to create {@link EPGMGraphHead}
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Knows how to create {@link EPGMVertex}
   */
  private final VertexFactory vertexFactory;

  /**
   *  Knows how to create {@link EPGMEdge}
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates a new Configuration.
   */
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
  public static GradoopConfig<EPGMGraphHead, EPGMVertex, EPGMEdge> getDefaultConfig() {
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
}
