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
package org.gradoop.temporal.model.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.model.api.TemporalGraphCollectionOperators;
import org.gradoop.temporal.model.impl.functions.EdgeFromTemporal;
import org.gradoop.temporal.model.impl.functions.GraphHeadFromTemporal;
import org.gradoop.temporal.model.impl.functions.VertexFromTemporal;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A temporal graph collection is a base concept of the Temporal Property Graph Model (TPGM)
 * that extends the Extended Property Graph Model (EPGM). The temporal graph collection
 * inherits the main concepts of the {@link org.gradoop.flink.model.impl.epgm.GraphCollection} and
 * extends them by temporal attributes. These attributes are two temporal information:
 * the valid-time and transaction time. Both are represented by a Tuple2 of Long values that specify
 * the beginning and end time as unix timestamp in milliseconds.
 * <p>
 * transactionTime: {@code (tx-from [ms], tx-to [ms])}<br>
 * validTime: {@code (val-from [ms], val-to [ms])}
 * <p>
 * Furthermore, a temporal graph collection provides operations that are performed on the underlying
 * data. These operations result in either another temporal graph collection or in a
 * {@link TemporalGraph}.
 * <p>
 * Analogous to a {@link org.gradoop.flink.model.impl.epgm.GraphCollection}, a temporal graph
 * collection is wrapping a layout which defines, how the graph is represented in Apache Flink.
 * Note that the {@link TemporalGraphCollection} also implements that interface and just forward
 * the calls to the layout. This is just for convenience and API synchronicity.
 */
public class TemporalGraphCollection implements BaseGraphCollection<
    TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph, TemporalGraphCollection>,
  GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>,
  TemporalGraphCollectionOperators {

  /**
   * Layout for that temporal graph.
   */
  private final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout;

  /**
   * Temporal Gradoop Configuration that holds all necessary factories and the execution environment.
   */
  private final TemporalGradoopConfig config;

  /**
   * Creates a new temporal graph instance with the given layout and temporal Gradoop configuration.
   *
   * @param layout the temporal graph layout representing the temporal graph
   * @param config Temporal Gradoop config
   */
  TemporalGraphCollection(
    GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout,
    TemporalGradoopConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public BaseGraphCollectionFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
    TemporalGraphCollection> getFactory() {
    return this.config.getTemporalGraphCollectionFactory();
  }

  @Override
  public BaseGraphFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
    TemporalGraphCollection> getGraphFactory() {
    return this.config.getTemporalGraphFactory();
  }

  /**
   * Returns a 1-element dataset containing a {@link Boolean} value which indicates if the graph collection
   * is empty.
   *
   * The graph is considered empty if it contains no graphs.
   *
   * @return 1-element dataset containing {@link Boolean#TRUE} if the collection is empty and
   * {@link Boolean#FALSE} otherwise.
   */
  public DataSet<Boolean> isEmpty() {
    return getGraphHeads().map(new True<>()).distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false)).reduce(new Or())
      .map(new Not());
  }

  /**
   * Writes the graph collection to the given data sink.
   *
   * @param dataSink The data sink to which the graph collection should be written.
   * @throws IOException if the collection can't be written to the sink
   */
  public void writeTo(TemporalDataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  /**
   * Writes the graph collection to the given data sink with an optional overwrite option.
   *
   * @param dataSink The data sink to which the graph collection should be written.
   * @param overWrite determines whether existing files are overwritten
   * @throws IOException if the collection can't be written to the sink
   */
  public void writeTo(TemporalDataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
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

  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------
  @Override
  public GraphCollection toGraphCollection() {
    final GraphCollectionFactory collectionFactory = this.config.getGraphCollectionFactory();
    return collectionFactory.fromDataSets(
      getGraphHeads().map(new GraphHeadFromTemporal(collectionFactory.getGraphHeadFactory())),
      getVertices().map(new VertexFromTemporal(collectionFactory.getVertexFactory())),
      getEdges().map(new EdgeFromTemporal(collectionFactory.getEdgeFactory())));
  }

  @Override
  public TemporalGraphCollection callForCollection(
    UnaryBaseGraphCollectionToBaseGraphCollectionOperator<TemporalGraphCollection> operator) {
    return operator.execute(this);
  }
}
