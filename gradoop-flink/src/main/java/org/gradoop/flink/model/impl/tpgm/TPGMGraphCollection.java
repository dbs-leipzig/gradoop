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
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.tpgm.TemporalGraphCollection;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.layouts.gve.temporal.TemporalGVELayout;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A concrete class representing a {@link TemporalGraphCollection} in the TPGM.
 */
public class TPGMGraphCollection implements TemporalGraphCollection {

  /**
   * Layout for that temporal graph.
   */
  private final TemporalGVELayout layout;
  /**
   * Gradoop Flink Configuration that holds all necessary factories and the execution environment.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new temporal graph instance with the given layout and gradoop flink configuration.
   *
   * @param layout the temporal graph layout representing the temporal graph
   * @param config Gradoop Flink configuration
   */
  TPGMGraphCollection(TemporalGVELayout layout, GradoopFlinkConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices().map(new True<>()).distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false)).reduce(new Or())
      .map(new Not());
  }

  @Override
  public void writeTo(DataSink dataSink) {
    throw new RuntimeException("Writing a temporal graph to a DataSink is not implemented yet.");
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) {
    throw new RuntimeException("Writing a temporal graph to a DataSink is not implemented yet.");
  }

  @Override
  public DataSet<TemporalVertex> getVertices() {
    return this.layout.getVertices();
  }

  @Override
  public DataSet<TemporalVertex> getVerticesByLabel(String label) {
    return this.layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<TemporalEdge> getEdges() {
    return this.layout.getEdges();
  }

  @Override
  public DataSet<TemporalEdge> getEdgesByLabel(String label) {
    return this.layout.getEdgesByLabel(label);
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeads() {
    return this.layout.getGraphHeads();
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeadsByLabel(String label) {
    return this.layout.getGraphHeadsByLabel(label);
  }
}
