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
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.tpgm.TemporalGraphCollectionOperators;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromTemporal;
import org.gradoop.flink.model.impl.functions.epgm.GraphHeadFromTemporal;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromTemporal;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * A temporal graph collection is a base concept of the Temporal Property Graph Model (TPGM)
 * that extends the Extended Property Graph Model (EPGM). The temporal graph collection
 * inherits the main concepts of the {@link org.gradoop.flink.model.impl.epgm.GraphCollection} and
 * extends them by temporal attributes. These attributes are two temporal information:
 * the valid-time and transaction time. Both are represented by a Tuple2 of Long values that specify
 * the beginning and end time as unix timestamp in milliseconds.
 *
 * transactionTime: (tx-from [ms], tx-to [ms])
 * validTime: (val-from [ms], val-to [ms])
 *
 * Furthermore, a temporal graph collection provides operations that are performed on the underlying
 * data. These operations result in either another temporal graph collection or in a
 * {@link TemporalGraph}.
 *
 * Analogous to a {@link org.gradoop.flink.model.impl.epgm.GraphCollection}, a temporal graph
 * collection is wrapping a layout which defines, how the graph is represented in Apache Flink.
 * Note that the {@link TemporalGraphCollection} also implements that interface and just forward
 * the calls to the layout. This is just for convenience and API synchronicity.
 */
public class TemporalGraphCollection implements
  BaseGraphCollection<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraphCollection>,
  GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>,
  TemporalGraphCollectionOperators {

  /**
   * Layout for that temporal graph.
   */
  private final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout;
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
  TemporalGraphCollection(
    GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout,
    GradoopFlinkConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public BaseGraphCollectionFactory<TemporalGraphHead, TemporalVertex, TemporalEdge,
    TemporalGraphCollection> getFactory() {
    return this.config.getTemporalGraphCollectionFactory();
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices().map(new True<>()).distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false)).reduce(new Or())
      .map(new Not());
  }

  @Override
  public void writeTo(DataSink dataSink) {
    throw new UnsupportedOperationException(
      "Writing a temporal graph to a DataSink is not implemented yet.");
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) {
    throw new UnsupportedOperationException(
      "Writing a temporal graph to a DataSink is not implemented yet.");
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
  public boolean isGVELayout() {
    return true;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public boolean isTransactionalLayout() {
    return false;
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeads() {
    return this.layout.getGraphHeads();
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeadsByLabel(String label) {
    return this.layout.getGraphHeadsByLabel(label);
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return this.layout.getGraphTransactions();
  }

  /**
   * Converts the {@link TemporalGraphCollection} to a {@link GraphCollection} instance by
   * discarding all temporal information from the graph elements. All Ids (graphs, vertices,
   * edges) are kept during the transformation.
   *
   * @return the graph collection instance
   */
  public GraphCollection toGraphCollection() {
    return getConfig().getGraphCollectionFactory().fromDataSets(
      getGraphHeads().map(new GraphHeadFromTemporal()),
      getVertices().map(new VertexFromTemporal()),
      getEdges().map(new EdgeFromTemporal()));
  }
}
