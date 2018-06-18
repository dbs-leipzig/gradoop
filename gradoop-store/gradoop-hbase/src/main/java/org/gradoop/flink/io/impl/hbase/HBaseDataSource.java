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
package org.gradoop.flink.io.impl.hbase;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.api.FilterableDataSource;
import org.gradoop.flink.io.impl.hbase.inputformats.EdgeTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.GraphHeadTableInputFormat;
import org.gradoop.flink.io.impl.hbase.inputformats.VertexTableInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.ValueOf1;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.annotation.Nonnull;

/**
 * Creates an EPGM instance from HBase.
 *
 * @see FilterableDataSource
 * TODO: make it implement FilterableDataSource
 */
public class HBaseDataSource extends HBaseBase implements DataSource {

  /**
   * Creates a new HBase data source.
   *
   * @param epgmStore HBase store
   * @param config    Gradoop Flink configuration
   */
  public HBaseDataSource(
    @Nonnull HBaseEPGMStore epgmStore,
    @Nonnull GradoopFlinkConfig config
  ) {
    super(epgmStore, config);
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  @SuppressWarnings("unchecked")
  public GraphCollection getGraphCollection() {
    GradoopHBaseConfig config = getHBaseConfig();
    HBaseEPGMStore store = getStore();

    VertexTableInputFormat<Vertex, Edge> vertexTableInputFormat = createVertexInput();
    EdgeTableInputFormat edgeTableInputFormat = createEdgeInput();

    DataSet<GraphHead> graphHeads = config.getExecutionEnvironment()
      .createInput(new GraphHeadTableInputFormat<>(config.getGraphHeadHandler(),
        store.getGraphHeadName()), new TupleTypeInfo<>(
        TypeExtractor.createTypeInfo(config.getGraphHeadFactory().getType())))
      .map(new ValueOf1<>());
    DataSet<Vertex> vertices = config.getExecutionEnvironment()
      .createInput(vertexTableInputFormat, new TupleTypeInfo<>(
        TypeExtractor.createTypeInfo(config.getVertexFactory().getType())))
      .map(new ValueOf1<>());
    DataSet<Edge> edges = config.getExecutionEnvironment()
      .createInput(edgeTableInputFormat, new TupleTypeInfo<>(
        TypeExtractor.createTypeInfo(config.getEdgeFactory().getType())))
      .map(new ValueOf1<>());

    return config.getGraphCollectionFactory()
      .fromDataSets(graphHeads, vertices, edges);
  }

  /**
   * create a vertex input format with filter predicate
   * @return vertex input instance
   */
  private VertexTableInputFormat<Vertex, Edge> createVertexInput() {
    VertexTableInputFormat<Vertex, Edge> input = new VertexTableInputFormat<>(
      getHBaseConfig().getVertexHandler(),
      getStore().getVertexTableName());
    //TODO: add your filter predicate expression handler here
    return input;
  }

  /**
   * create a edge input format with filter predicate
   * @return edge input instance
   */
  private EdgeTableInputFormat<Edge, Vertex> createEdgeInput() {
    EdgeTableInputFormat<Edge, Vertex> input =
      new EdgeTableInputFormat<>(
        getHBaseConfig().getEdgeHandler(),
        getStore().getEdgeTableName());
    //TODO: add your filter predicate expression handler here
    return input;
  }

}
