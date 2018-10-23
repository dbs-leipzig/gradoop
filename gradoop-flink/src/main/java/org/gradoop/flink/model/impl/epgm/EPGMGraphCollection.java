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
package org.gradoop.flink.model.impl.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Objects;

/**
 * A concrete class representing a {@link GraphCollection} in the EPGM.
 */
public class EPGMGraphCollection implements GraphCollection {

  /**
   * Layout for that graph collection
   */
  private final GraphCollectionLayout layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param layout Graph collection layout
   * @param config Gradoop Flink configuration
   */
  public EPGMGraphCollection(GraphCollectionLayout layout, GradoopFlinkConfig config) {
    Objects.requireNonNull(layout);
    Objects.requireNonNull(config);
    this.layout = layout;
    this.config = config;
  }

  //----------------------------------------------------------------------------
  // Data methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public boolean isGVELayout() {
    return layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return layout.isIndexedGVELayout();
  }

  @Override
  public boolean isTransactionalLayout() {
    return layout.isTransactionalLayout();
  }

  @Override
  public DataSet<Vertex> getVertices() {
    return layout.getVertices();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    return layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<Edge> getEdges() {
    return layout.getEdges();
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    return layout.getEdgesByLabel(label);
  }

  @Override
  public DataSet<GraphHead> getGraphHeads() {
    return layout.getGraphHeads();
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    return layout.getGraphHeadsByLabel(label);
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return layout.getGraphTransactions();
  }

}
