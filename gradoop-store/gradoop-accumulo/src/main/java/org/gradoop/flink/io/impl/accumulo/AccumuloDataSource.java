/**
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

package org.gradoop.flink.io.impl.accumulo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.common.storage.impl.accumulo.predicate.query.AccumuloQueryHolder;
import org.gradoop.common.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.common.storage.predicate.query.ElementQuery;
import org.gradoop.flink.io.api.FilterableDataSource;
import org.gradoop.flink.io.impl.accumulo.inputformats.EdgeInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.GraphHeadInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.VertexInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;

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
   */
  public AccumuloDataSource(@Nonnull AccumuloEPGMStore store) {
    this(store, null, null, null);
  }

  /**
   * Creates a new Accumulo data source.
   *
   * @param store accumulo epgm store
   * @param graphQuery graph head filter
   * @param vertexQuery vertex filter
   * @param edgeQuery edge filter
   */
  private AccumuloDataSource(
    @Nonnull AccumuloEPGMStore store,
    @Nullable AccumuloQueryHolder<GraphHead> graphQuery,
    @Nullable AccumuloQueryHolder<Vertex> vertexQuery,
    @Nullable AccumuloQueryHolder<Edge> edgeQuery
  ) {
    super(store);
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
    GraphCollectionFactory factory = getAccumuloConfig().getGraphCollectionFactory();
    ExecutionEnvironment env = getAccumuloConfig().getExecutionEnvironment();
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
