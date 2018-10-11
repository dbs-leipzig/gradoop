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
package org.gradoop.flink.model.impl.layouts.gve.temporal;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.impl.layouts.gve.GVELayout;

/**
 * TODO: descriptions
 */
public class TemporalLayout extends GVELayout {
  /**
   * Mapping from graph label to graph heads with that label.
   */
  private final DataSet<TemporalGraphHead> temporalGraphHeads;
  /**
   * Mapping from vertex label to vertices with that label.
   */
  private final DataSet<TemporalVertex> temporalVertices;
  /**
   * Mapping from edge label to edges with that label.
   */
  private final DataSet<TemporalEdge> temporalEdges;

  /**
   * Creates a new temporal layout holding the graph elements.
   *
   * @param temporalGraphHeads graph head dataset
   * @param temporalVertices vertex dataset
   * @param temporalEdges edge dataset
   */
  protected TemporalLayout(DataSet<TemporalGraphHead> temporalGraphHeads,
    DataSet<TemporalVertex> temporalVertices, DataSet<TemporalEdge> temporalEdges) {
    super(null,null,null);

    this.temporalGraphHeads = temporalGraphHeads;
    this.temporalVertices = temporalVertices;
    this.temporalEdges = temporalEdges;
  }

  public DataSet<TemporalGraphHead> getTemporalGraphHeads() {
    return temporalGraphHeads;
  }

  public DataSet<TemporalVertex> getTemporalVertices() {
    return temporalVertices;
  }

  public DataSet<TemporalEdge> getTemporalEdges() {
    return temporalEdges;
  }

  /*
  @Override
  public DataSet<GraphHead> getGraphHeads() {
    return this.graphHeads.map(g -> g.);
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return this.graphHeads.filter().map();
  }

  @Override
  public DataSet<GraphHead> getGraphHead() {
    return this.graphHeads.map();
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return this.vertices.map();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return this.vertices.filter().map();
  }

  @Override
  public DataSet<Edge> getEdges() {
    return this.edges.map(TemporalEdge::toEdge);
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return this.edges.filter().map();
  }*/


}
