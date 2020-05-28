/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraphOperators;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToValueOperator;
import org.gradoop.flink.model.api.operators.GraphsToGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.PropertyGetter;
import org.gradoop.flink.model.impl.operators.rollup.EdgeRollUp;
import org.gradoop.flink.model.impl.operators.rollup.VertexRollUp;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;
import org.gradoop.flink.model.impl.operators.split.Split;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A logical graph is one of the base concepts of the Extended Property Graph Model. A logical graph
 * encapsulates three concepts:
 * <ul>
 * <li>a graph head, that stores information about the graph (i.e. label and properties)</li>
 * <li>a set of vertices assigned to the graph</li>
 * <li>a set of directed, possibly parallel edges assigned to the graph</li>
 * </ul>
 * Furthermore, a logical graph provides operations that are performed on the underlying data. These
 * operations result in either another logical graph or in a {@link GraphCollection}.
 * <p>
 * A logical graph is wrapping a {@link LogicalGraphLayout} which defines, how the graph is
 * represented in Apache Flink. Note that the LogicalGraph also implements that interface and
 * just forward the calls to the layout. This is just for convenience and API synchronicity.
 */
public class LogicalGraph implements
  BaseGraph<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>, LogicalGraphOperators {
  /**
   * Layout for that logical graph.
   */
  private final LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param layout representation of the logical graph
   * @param config the Gradoop Flink configuration
   */
  LogicalGraph(LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> layout,
    GradoopFlinkConfig config) {
    Objects.requireNonNull(layout);
    Objects.requireNonNull(config);
    this.layout = layout;
    this.config = config;
  }

  //----------------------------------------------------------------------------
  // Data methods
  //----------------------------------------------------------------------------

  @Override
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  @Override
  public BaseGraphFactory<
    EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> getFactory() {
    return config.getLogicalGraphFactory();
  }

  @Override
  public BaseGraphCollectionFactory<
    EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> getCollectionFactory() {
    return config.getGraphCollectionFactory();
  }

  @Override
  public boolean isGVELayout() {
    return layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return layout.isIndexedGVELayout();
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHead() {
    return layout.getGraphHead();
  }

  @Override
  public DataSet<EPGMVertex> getVertices() {
    return layout.getVertices();
  }

  @Override
  public DataSet<EPGMVertex> getVerticesByLabel(String label) {
    return layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<EPGMEdge> getEdges() {
    return layout.getEdges();
  }

  @Override
  public DataSet<EPGMEdge> getEdgesByLabel(String label) {
    return layout.getEdgesByLabel(label);
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph sample(SamplingAlgorithm algorithm) {
    return callForGraph(algorithm);
  }

  @Override
  public GraphCollection groupVerticesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions) {
    if (vertexGroupingKeys == null || vertexGroupingKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing vertex grouping key(s).");
    }

    return callForCollection(new VertexRollUp(vertexGroupingKeys, vertexAggregateFunctions,
      edgeGroupingKeys, edgeAggregateFunctions));
  }

  @Override
  public GraphCollection groupEdgesByRollUp(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions) {
    if (edgeGroupingKeys == null || edgeGroupingKeys.isEmpty()) {
      throw new IllegalArgumentException("Missing edge grouping key(s).");
    }

    return callForCollection(new EdgeRollUp(vertexGroupingKeys, vertexAggregateFunctions,
      edgeGroupingKeys, edgeAggregateFunctions));
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public <T> T callForValue(UnaryBaseGraphToValueOperator<LogicalGraph, T> operator) {
    return operator.execute(this);
  }

  @Override
  public <T> T callForValue(BinaryBaseGraphToValueOperator<LogicalGraph, T> operator,
                            LogicalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  @Override
  public LogicalGraph callForGraph(GraphsToGraphOperator operator,
    LogicalGraph... otherGraphs) {
    return operator.execute(this, otherGraphs);
  }

  @Override
  public GraphCollection splitBy(String propertyKey) {
    return callForCollection(new Split(new PropertyGetter<>(Lists.newArrayList(propertyKey))));
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  /**
   * Prints the GDL formatted graph to the standard output.
   *
   * @throws Exception forwarded from dataset print
   */
  public void print() throws Exception {
    GDLConsoleOutput.print(this);
  }
}
