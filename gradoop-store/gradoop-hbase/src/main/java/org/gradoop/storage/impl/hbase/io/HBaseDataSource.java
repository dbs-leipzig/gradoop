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
package org.gradoop.storage.impl.hbase.io;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.io.FilterableDataSource;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.io.inputformats.EdgeTableInputFormat;
import org.gradoop.storage.impl.hbase.io.inputformats.GraphHeadTableInputFormat;
import org.gradoop.storage.impl.hbase.io.inputformats.VertexTableInputFormat;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Creates an EPGM instance from HBase.
 *
 * @see FilterableDataSource
 */
public class HBaseDataSource extends HBaseBase
  implements FilterableDataSource<
  ElementQuery<HBaseElementFilter<GraphHead>>,
  ElementQuery<HBaseElementFilter<Vertex>>,
  ElementQuery<HBaseElementFilter<Edge>>> {

  /**
   * Query definition for graph head elements
   */
  private final ElementQuery<HBaseElementFilter<GraphHead>> graphHeadQuery;

  /**
   * Query definition for vertices
   */
  private final ElementQuery<HBaseElementFilter<Vertex>> vertexQuery;

  /**
   * Query definition for edges
   */
  private final ElementQuery<HBaseElementFilter<Edge>> edgeQuery;

  /**
   * Creates a new HBase data source.
   *
   * @param epgmStore HBase store
   * @param flinkConfig gradoop flink execute config
   */
  public HBaseDataSource(
    @Nonnull HBaseEPGMStore epgmStore,
    @Nonnull GradoopFlinkConfig flinkConfig
  ) {
    this(epgmStore, flinkConfig, null, null, null);
  }

  /**
   * Private constructor to create a data source instance with predicates.
   *
   * @param epgmStore HBase store
   * @param flinkConfig gradoop flink execute config
   * @param graphHeadQuery A predicate to apply to graph head elements
   * @param vertexQuery A predicate to apply to vertices
   * @param edgeQuery A predicate to apply to edges
   */
  private HBaseDataSource(
    @Nonnull HBaseEPGMStore epgmStore,
    @Nonnull GradoopFlinkConfig flinkConfig,
    @Nullable ElementQuery<HBaseElementFilter<GraphHead>> graphHeadQuery,
    @Nullable ElementQuery<HBaseElementFilter<Vertex>> vertexQuery,
    @Nullable ElementQuery<HBaseElementFilter<Edge>> edgeQuery
  ) {
    super(epgmStore, flinkConfig);
    this.graphHeadQuery = graphHeadQuery;
    this.vertexQuery = vertexQuery;
    this.edgeQuery = edgeQuery;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection getGraphCollection() {
    GradoopFlinkConfig config = getFlinkConfig();
    HBaseEPGMStore store = getStore();

    DataSet<GraphHead> graphHeads = config.getExecutionEnvironment()
      .createInput(new GraphHeadTableInputFormat(
          getHBaseConfig().getGraphHeadHandler().applyQuery(graphHeadQuery),
          store.getGraphHeadName()),
        new TupleTypeInfo<>(TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType())))
      .map(new ValueOf1<>());

    DataSet<Vertex> vertices = config.getExecutionEnvironment()
      .createInput(new VertexTableInputFormat(
          getHBaseConfig().getVertexHandler().applyQuery(vertexQuery),
          store.getVertexTableName()),
        new TupleTypeInfo<>(TypeExtractor.createTypeInfo(config.getVertexFactory().getType())))
      .map(new ValueOf1<>());

    DataSet<Edge> edges = config.getExecutionEnvironment()
      .createInput(new EdgeTableInputFormat(
          getHBaseConfig().getEdgeHandler().applyQuery(edgeQuery),
          store.getEdgeTableName()),
        new TupleTypeInfo<>(TypeExtractor.createTypeInfo(config.getEdgeFactory().getType())))
      .map(new ValueOf1<>());

    return config.getGraphCollectionFactory().fromDataSets(graphHeads, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public HBaseDataSource applyGraphPredicate(
    @Nonnull ElementQuery<HBaseElementFilter<GraphHead>> query
  ) {
    return new HBaseDataSource(getStore(), getFlinkConfig(), query, vertexQuery, edgeQuery);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public HBaseDataSource applyVertexPredicate(
    @Nonnull ElementQuery<HBaseElementFilter<Vertex>> query
  ) {
    return new HBaseDataSource(getStore(), getFlinkConfig(), graphHeadQuery, query, edgeQuery);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public HBaseDataSource applyEdgePredicate(
    @Nonnull ElementQuery<HBaseElementFilter<Edge>> query
  ) {
    return new HBaseDataSource(getStore(), getFlinkConfig(), graphHeadQuery, vertexQuery, query);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isFilterPushedDown() {
    return this.graphHeadQuery != null || this.vertexQuery != null || this.edgeQuery != null;
  }
}
