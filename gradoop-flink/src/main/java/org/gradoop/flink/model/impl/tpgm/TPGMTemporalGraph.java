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
package org.gradoop.flink.model.impl.tpgm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.layouts.gve.temporal.TemporalLayout;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * TODO: descriptions
 */
public class TPGMTemporalGraph implements TemporalGraph {

  /**
   * Layout for that logical graph.
   */
  private final TemporalLayout layout;
  /**
   * Gradoop Flink Configuration that holds
   */
  private final GradoopFlinkConfig config;

  public TPGMTemporalGraph(TemporalLayout layout, GradoopFlinkConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public boolean isGVELayout() {
    return false;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public DataSet<GraphHead> getGraphHead() {
    return null;
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return this.layout.getVertices();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return this.layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<Edge> getEdges() {
    return this.layout.getEdges();
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return this.layout.getEdgesByLabel(label);
  }

  @Override
  public TemporalGraph toTemporalGraph() {
    return this;
  }
}
