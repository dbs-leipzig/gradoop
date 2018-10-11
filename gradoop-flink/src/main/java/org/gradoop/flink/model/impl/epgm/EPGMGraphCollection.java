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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.distinction.DistinctById;
import org.gradoop.flink.model.impl.operators.distinction.DistinctByIsomorphism;
import org.gradoop.flink.model.impl.operators.distinction.GroupByIsomorphism;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Collections;
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

  //----------------------------------------------------------------------------
  // Logical Graph / Graph Head Getters
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph getGraph(final GradoopId graphID) {
    // filter vertices and edges based on given graph id
    DataSet<GraphHead> graphHead = getGraphHeads()
      .filter(new BySameId<>(graphID));
    DataSet<Vertex> vertices = getVertices()
      .filter(new InGraph<>(graphID));
    DataSet<Edge> edges = getEdges()
      .filter(new InGraph<>(graphID));

    return new EPGMLogicalGraph(
      config.getLogicalGraphFactory().fromDataSets(graphHead, vertices, edges),
      getConfig());
  }

  @Override
  public GraphCollection getGraphs(final GradoopId... identifiers) {

    GradoopIdSet graphIds = new GradoopIdSet();

    Collections.addAll(graphIds, identifiers);

    return getGraphs(graphIds);
  }

  @Override
  public GraphCollection getGraphs(final GradoopIdSet identifiers) {

    DataSet<GraphHead> newGraphHeads = this.getGraphHeads()
      .filter((FilterFunction<GraphHead>) graphHead -> identifiers.contains(graphHead.getId()));

    // build new vertex set
    DataSet<Vertex> vertices = getVertices()
      .filter(new InAnyGraph<>(identifiers));

    // build new edge set
    DataSet<Edge> edges = getEdges()
      .filter(new InAnyGraph<>(identifiers));

    return new EPGMGraphCollection(
      getConfig().getGraphCollectionFactory().fromDataSets(newGraphHeads, vertices, edges),
      getConfig());
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return getGraphHeads()
      .map(new True<>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  @Override
  public GraphCollection distinctById() {
    return callForCollection(new DistinctById());
  }

  @Override
  public GraphCollection distinctByIsomorphism() {
    return callForCollection(new DistinctByIsomorphism());
  }

  @Override
  public GraphCollection groupByIsomorphism(GraphHeadReduceFunction func) {
    return callForCollection(new GroupByIsomorphism(func));
  }

  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

}
