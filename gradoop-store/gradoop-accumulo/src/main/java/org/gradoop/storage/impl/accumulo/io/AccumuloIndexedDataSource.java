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

package org.gradoop.storage.impl.accumulo.io;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.io.IndexedDataSource;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.functions.ElementMapFunction;
import org.gradoop.storage.impl.accumulo.io.inputformats.EdgeInputFormat;
import org.gradoop.storage.impl.accumulo.io.inputformats.GraphHeadInputFormat;
import org.gradoop.storage.impl.accumulo.io.inputformats.VertexInputFormat;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Accumulo Indexed Datasource
 */
public class AccumuloIndexedDataSource extends AccumuloBase implements
  IndexedDataSource<
    AccumuloElementFilter<GraphHead>,
    AccumuloElementFilter<Vertex>,
    AccumuloElementFilter<Edge>> {

  /**
   * Creates a new Accumulo indexed data source.
   *
   * @param flinkConfig gradoop flink configuration
   * @param store accumulo epgm store
   */
  public AccumuloIndexedDataSource(
    @Nonnull GradoopFlinkConfig flinkConfig,
    @Nonnull AccumuloEPGMStore store
  ) {
    super(store, flinkConfig);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory factory = getFlinkConfig().getGraphCollectionFactory();
    ExecutionEnvironment env = getFlinkConfig().getExecutionEnvironment();

    GraphHeadInputFormat graphHeadInputFormat = new GraphHeadInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().noFilter()));
    VertexInputFormat vertexInputFormat = new VertexInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().noFilter()));
    EdgeInputFormat edgeInputFormat = new EdgeInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().noFilter()));

    return factory.fromDataSets(
      env.createInput(graphHeadInputFormat),
      env.createInput(vertexInputFormat),
      env.createInput(edgeInputFormat));
  }

  @Nonnull
  @Override
  public DataSet<GraphHead> getGraphHeads(
    @Nonnull AccumuloElementFilter<GraphHead> filter
  ) {
    ExecutionEnvironment env = getFlinkConfig().getExecutionEnvironment();
    GraphHeadInputFormat graphHeadInputFormat = new GraphHeadInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().where(filter)));
    return env.createInput(graphHeadInputFormat);
  }

  @Nonnull
  @Override
  public DataSet<Vertex> getVertices(
    @Nonnull AccumuloElementFilter<Vertex> filter
  ) {
    ExecutionEnvironment env = getFlinkConfig().getExecutionEnvironment();
    VertexInputFormat vertexInputFormat = new VertexInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().where(filter)));
    return env.createInput(vertexInputFormat);
  }

  @Nonnull
  @Override
  public DataSet<Edge> getEdges(
    @Nonnull AccumuloElementFilter<Edge> filter
  ) {
    ExecutionEnvironment env = getFlinkConfig().getExecutionEnvironment();
    EdgeInputFormat edgeInputFormat = new EdgeInputFormat(
      getStore().getConfig().getAccumuloProperties(),
      AccumuloQueryHolder.create(Query.elements().fromAll().where(filter)));
    return env.createInput(edgeInputFormat);
  }

  @Nonnull
  @Override
  public DataSet<GraphHead> getGraphHeads(
    @Nonnull DataSet<GradoopId> graphHeadIds,
    @Nullable AccumuloElementFilter<GraphHead> filter
  ) {
    return graphHeadIds
      .mapPartition(new ElementMapFunction<>(GraphHead.class, getAccumuloConfig(), filter))
      .returns(new TypeHint<GraphHead>() {
      });
  }

  @Nonnull
  @Override
  public DataSet<Vertex> getVertices(
    @Nonnull DataSet<GradoopId> vertexIds,
    @Nullable AccumuloElementFilter<Vertex> filter
  ) {
    return vertexIds
      .mapPartition(new ElementMapFunction<>(Vertex.class, getAccumuloConfig(), filter))
      .returns(new TypeHint<Vertex>() {
      });
  }

  @Nonnull
  @Override
  public DataSet<Edge> getEdges(
    @Nonnull DataSet<GradoopId> edgeIds,
    @Nullable AccumuloElementFilter<Edge> filter
  ) {
    return edgeIds
      .mapPartition(new ElementMapFunction<>(Edge.class, getAccumuloConfig(), filter))
      .returns(new TypeHint<Edge>() {
      });
  }

}
