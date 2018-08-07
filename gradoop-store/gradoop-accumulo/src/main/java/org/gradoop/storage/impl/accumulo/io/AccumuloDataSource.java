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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.io.FilterableDataSource;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.io.inputformats.EdgeInputFormat;
import org.gradoop.storage.impl.accumulo.io.inputformats.GraphHeadInputFormat;
import org.gradoop.storage.impl.accumulo.io.inputformats.VertexInputFormat;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Read logic graph or graph collection from accumulo store
 */
public class AccumuloDataSource extends AccumuloBase implements FilterableDataSource<
  ElementQuery<AccumuloElementFilter<GraphHead>>,
  ElementQuery<AccumuloElementFilter<Vertex>>,
  ElementQuery<AccumuloElementFilter<Edge>>> {

  /**
   * graph head filter
   */
  private final AccumuloQueryHolder<GraphHead> graphHeadQuery;

  /**
   * vertex filter
   */
  private final AccumuloQueryHolder<Vertex> vertexQuery;

  /**
   * edge filter
   */
  private final AccumuloQueryHolder<Edge> edgeQuery;

  /**
   * Creates a new Accumulo data source.
   *
   * @param store accumulo epgm store
   * @param config gradoop flink configuration
   */
  public AccumuloDataSource(
    @Nonnull AccumuloEPGMStore store,
    @Nonnull GradoopFlinkConfig config
  ) {
    this(store, config, null, null, null);
  }

  /**
   * Creates a new Accumulo data source.
   *
   * @param store accumulo epgm store
   * @param config gradoop flink configuration
   * @param graphQuery graph head filter
   * @param vertexQuery vertex filter
   * @param edgeQuery edge filter
   */
  private AccumuloDataSource(
    @Nonnull AccumuloEPGMStore store,
    @Nonnull GradoopFlinkConfig config,
    @Nullable AccumuloQueryHolder<GraphHead> graphQuery,
    @Nullable AccumuloQueryHolder<Vertex> vertexQuery,
    @Nullable AccumuloQueryHolder<Edge> edgeQuery
  ) {
    super(store, config);
    this.graphHeadQuery = graphQuery;
    this.vertexQuery = vertexQuery;
    this.edgeQuery = edgeQuery;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory factory = getFlinkConfig().getGraphCollectionFactory();
    ExecutionEnvironment env = getFlinkConfig().getExecutionEnvironment();
    return factory.fromDataSets(
      /*graph head format*/
      env.createInput(new GraphHeadInputFormat(
        getStore().getConfig().getAccumuloProperties(),
        graphHeadQuery)),
      /*vertex input format*/
      env.createInput(new VertexInputFormat(getStore().getConfig().getAccumuloProperties(),
        vertexQuery)),
      /*edge input format*/
      env.createInput(new EdgeInputFormat(getStore().getConfig().getAccumuloProperties(),
        edgeQuery)));
  }

  @Nonnull
  @Override
  public AccumuloDataSource applyGraphPredicate(
    @Nonnull ElementQuery<AccumuloElementFilter<GraphHead>> query
  ) {
    AccumuloQueryHolder<GraphHead> newGraphQuery = AccumuloQueryHolder.create(query);
    return new AccumuloDataSource(
      getStore(),
      getFlinkConfig(),
      newGraphQuery,
      vertexQuery,
      edgeQuery
    );
  }

  @Nonnull
  @Override
  public AccumuloDataSource applyVertexPredicate(
    @Nonnull ElementQuery<AccumuloElementFilter<Vertex>> query
  ) {
    AccumuloQueryHolder<Vertex> newVertexQuery = AccumuloQueryHolder.create(query);
    return new AccumuloDataSource(
      getStore(),
      getFlinkConfig(),
      graphHeadQuery,
      newVertexQuery,
      edgeQuery
    );
  }

  @Nonnull
  @Override
  public AccumuloDataSource applyEdgePredicate(
    @Nonnull ElementQuery<AccumuloElementFilter<Edge>> query
  ) {
    AccumuloQueryHolder<Edge> newEdgeQuery = AccumuloQueryHolder.create(query);
    return new AccumuloDataSource(
      getStore(),
      getFlinkConfig(),
      graphHeadQuery,
      vertexQuery,
      newEdgeQuery
    );
  }

  @Override
  public boolean isFilterPushedDown() {
    return this.graphHeadQuery != null ||
      this.vertexQuery != null ||
      this.edgeQuery != null;
  }

}
