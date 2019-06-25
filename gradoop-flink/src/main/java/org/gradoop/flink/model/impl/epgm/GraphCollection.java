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
package org.gradoop.flink.model.impl.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.epgm.GraphCollectionOperators;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * A graph collection graph is one of the base concepts of the Extended Property Graph Model. From
 * a model perspective, the collection represents a set of logical graphs. From a data perspective
 * this is reflected by providing three concepts:
 * <ul>
 * <li>a set of graph heads assigned to the graphs in that collection</li>
 * <li>a set of vertices which is the union of all vertex sets of the represented graphs</li>
 * <li>a set of edges which is the union of all edge sets of the represented graphs</li>
 * </ul>
 * Furthermore, a graph collection provides operations that are performed on the underlying data.
 * These operations result in either another graph collection or in a {@link LogicalGraph}.
 * <p>
 * A graph collection is wrapping a {@link GraphCollectionLayout} which defines, how the collection
 * is represented in Apache Flink. Note that the GraphCollection also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public class GraphCollection implements
  BaseGraphCollection<GraphHead, Vertex, Edge, LogicalGraph, GraphCollection>,
  GraphCollectionOperators {
  /**
   * Layout for that graph collection
   */
  private final GraphCollectionLayout<GraphHead, Vertex, Edge> layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param layout the graph collection layout
   * @param config the Gradoop Flink configuration
   */
  GraphCollection(GraphCollectionLayout<GraphHead, Vertex, Edge> layout,
    GradoopFlinkConfig config) {
    this.layout = Objects.requireNonNull(layout);
    this.config = Objects.requireNonNull(config);
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
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public GraphCollection callForCollection(
    UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GraphCollection> operator) {
    return operator.execute(this);
  }

  @Override
  public GraphCollection callForCollection(
    BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GraphCollection> operator,
    GraphCollection otherCollection) {
    return operator.execute(this, otherCollection);
  }

  @Override
  public <T> T callForCollection(
    BinaryBaseGraphCollectionToValueOperator<GraphCollection, T> operator,
    GraphCollection otherCollection) {
    return operator.execute(this, otherCollection);
  }

  @Override
  public LogicalGraph callForGraph(
    UnaryBaseGraphCollectionToBaseGraphOperator<GraphCollection, LogicalGraph> operator) {
    return operator.execute(this);
  }

  @Override
  public GraphCollection apply(ApplicableUnaryGraphToGraphOperator op) {
    return callForCollection(op);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public BaseGraphCollectionFactory<GraphHead, Vertex, Edge, LogicalGraph, GraphCollection>
  getFactory() {
    return config.getGraphCollectionFactory();
  }

  @Override
  public BaseGraphFactory<GraphHead, Vertex, Edge, LogicalGraph, GraphCollection>
  getGraphFactory() {
    return config.getLogicalGraphFactory();
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
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  /**
   * Prints this graph collection to the console.
   *
   * @throws Exception forwarded DataSet print() Exception.
   */
  public void print() throws Exception {
    GDLConsoleOutput.print(this);
  }
}
