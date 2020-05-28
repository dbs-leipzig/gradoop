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
package org.gradoop.temporal.model.impl;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToValueOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.model.api.TemporalGraphOperators;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalEdgeToEdge;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalVertexToVertex;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalGraphHeadToGraphHead;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalEdgeToDataString;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalGraphHeadToDataString;
import org.gradoop.temporal.model.impl.operators.tostring.TemporalVertexToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A temporal (logical) graph is a base concept of the Temporal Property Graph Model (TPGM) that
 * extends the Extended Property Graph Model (EPGM). The temporal graph inherits the main concepts
 * of the {@link org.gradoop.flink.model.impl.epgm.LogicalGraph} and extends them by temporal
 * attributes. These attributes are two temporal intervals: the valid-time and transaction time.
 * Both are represented by a {@link Tuple2} of Long values that specify the beginning and end time as unix
 * timestamp in milliseconds.
 *
 * <ul>
 *   <li>{@code transactionTime}: {@code (tx-from [ms], tx-to [ms])}</li>
 *   <li>{@code validTime}: {@code (val-from [ms], val-to [ms])}</li>
 * </ul>
 *
 * Furthermore, a temporal graph provides operations that are performed on the underlying data.
 * These operations result in either another temporal graph or in a {@link TemporalGraphCollection}.
 *
 * Analogous to a logical graph, a temporal graph is wrapping a layout which defines, how the graph
 * is represented in Apache Flink.<br>
 * Note that the {@link TemporalGraph} also implements that interface and just forwards the calls to
 * the layout. This is just for convenience and API synchronicity.
 *
 * @see TemporalGraphOperators
 */
public class TemporalGraph implements BaseGraph<TemporalGraphHead, TemporalVertex, TemporalEdge,
  TemporalGraph, TemporalGraphCollection>, TemporalGraphOperators {

  /**
   * Layout for that temporal graph.
   */
  private final LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout;
  /**
   * Configuration
   */
  private final TemporalGradoopConfig config;

  /**
   * Creates a new temporal graph instance with the given layout and temporal gradoop configuration.
   *
   * @param layout the layout representing the temporal graph
   * @param config the temporal Gradoop config
   */
  TemporalGraph(LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout,
    TemporalGradoopConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public TemporalGradoopConfig getConfig() {
    return this.config;
  }

  @Override
  public BaseGraphFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> getFactory() {
    return this.config.getTemporalGraphFactory();
  }

  @Override
  public BaseGraphCollectionFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> getCollectionFactory() {
    return this.config.getTemporalGraphCollectionFactory();
  }

  /**
   * Writes the graph to given data sink.
   *
   * @param dataSink The data sink to which the graph should be written.
   * @throws IOException if the graph can't be written to the sink
   */
  public void writeTo(TemporalDataSink dataSink) throws IOException {
    dataSink.write(this);
  }

  /**
   * Writes the graph to given data sink with an optional overwrite option.
   *
   * @param dataSink The data sink to which the graph should be written.
   * @param overWrite determines whether existing files are overwritten
   * @throws IOException if the graph can't be written to the sink
   */
  public void writeTo(TemporalDataSink dataSink, boolean overWrite) throws IOException {
    dataSink.write(this, overWrite);
  }

  @Override
  public boolean isGVELayout() {
    return this.layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return this.layout.isIndexedGVELayout();
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHead() {
    return this.layout.getGraphHead();
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

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  public DataSet<Boolean> equalsByElementData(TemporalGraph other) {
    return callForValue(new GraphEquality<>(
      new GraphHeadToEmptyString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(), true), other);
  }

  @Override
  public DataSet<Boolean> equalsByData(TemporalGraph other) {
    return callForValue(new GraphEquality<>(
      new TemporalGraphHeadToDataString<>(),
      new TemporalVertexToDataString<>(),
      new TemporalEdgeToDataString<>(), true), other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public <T> T callForValue(UnaryBaseGraphToValueOperator<TemporalGraph, T> operator) {
    return operator.execute(this);
  }

  @Override
  public <T> T callForValue(BinaryBaseGraphToValueOperator<TemporalGraph, T> operator,
                            TemporalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph toLogicalGraph() {
    final LogicalGraphFactory logicalGraphFactory = getConfig().getLogicalGraphFactory();
    return logicalGraphFactory.fromDataSets(
      getGraphHead().map(new TemporalGraphHeadToGraphHead(logicalGraphFactory.getGraphHeadFactory())),
      getVertices().map(new TemporalVertexToVertex(logicalGraphFactory.getVertexFactory())),
      getEdges().map(new TemporalEdgeToEdge(logicalGraphFactory.getEdgeFactory())));
  }

  /**
   * Convenience API function to create a {@link TemporalGraph} from an existing {@link BaseGraph} with
   * default values for the temporal attributes.
   *
   * @param baseGraph the existing graph instance
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The type of the graph.
   * @param <GC> The type of the Graph collection
   * @return a temporal graph with default temporal values
   * @see TemporalGraphFactory#fromNonTemporalGraph(BaseGraph)
   */
  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> TemporalGraph fromGraph(LG baseGraph) {
    return TemporalGradoopConfig.fromGradoopFlinkConfig(baseGraph.getConfig()).getTemporalGraphFactory()
      .fromNonTemporalGraph(baseGraph);
  }

  /**
   * Function to create a {@link TemporalGraph} from an existing {@link BaseGraph} with valid times
   * depending on the three given {@link TimeIntervalExtractor} instances
   *
   * @param baseGraph the existing Graph
   * @param graphTimeExtractor mapFunction to generate valid times for graphHead
   * @param vertexTimeExtractor mapFunction to generate valid times for vertices
   * @param edgeTimeExtractor mapFunction to generate valid times for edges
   * @param <G> The graph head type.
   * @param <V> The vertex type.
   * @param <E> The edge type.
   * @param <LG> The type of the graph.
   * @param <GC> The type of the Graph collection
   * @return a temporal graph with new valid time values
   */
  public static <
    G extends GraphHead,
    V extends Vertex, E extends Edge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, LG, GC>> TemporalGraph fromGraph(LG baseGraph,
      TimeIntervalExtractor<G> graphTimeExtractor,
      TimeIntervalExtractor<V> vertexTimeExtractor,
      TimeIntervalExtractor<E> edgeTimeExtractor) {
    TemporalGradoopConfig temporalGradoopConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(
      baseGraph.getConfig());
    return temporalGradoopConfig.getTemporalGraphFactory().fromNonTemporalDataSets(
      baseGraph.getGraphHead(), graphTimeExtractor, baseGraph.getVertices(), vertexTimeExtractor,
      baseGraph.getEdges(), edgeTimeExtractor);
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
