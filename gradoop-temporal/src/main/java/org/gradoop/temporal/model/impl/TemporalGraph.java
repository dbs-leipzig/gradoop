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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.gradoop.flink.io.impl.gdl.GDLConsoleOutput;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.model.api.TemporalGraphOperators;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalEdgeToEdge;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalGraphHeadToGraphHead;
import org.gradoop.temporal.model.impl.functions.tpgm.TemporalVertexToVertex;
import org.gradoop.temporal.model.impl.operators.diff.Diff;
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
   * Returns a 1-element dataset containing a {@link Boolean} value which indicates if the graph is empty.
   *
   * A graph is considered empty, if it contains no vertices.
   *
   * @return  1-element dataset containing {@link Boolean#TRUE}, if the collection is
   *          empty or {@link Boolean#FALSE} if not
   */
  public DataSet<Boolean> isEmpty() {
    return getVertices().map(new True<>()).distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false)).reduce(new Or())
      .map(new Not());
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
  public TemporalGraph diff(TemporalPredicate firstSnapShot, TemporalPredicate secondSnapshot) {
    return callForGraph(new Diff(firstSnapShot, secondSnapshot));
  }

  @Override
  public TemporalGraphCollection query(String query, String constructionPattern,
    GraphStatistics graphStatistics) {
    return callForCollection(new CypherPatternMatching<>(query, constructionPattern,
      true, MatchStrategy.HOMOMORPHISM, MatchStrategy.ISOMORPHISM, graphStatistics));
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public TemporalGraph callForGraph(UnaryBaseGraphToBaseGraphOperator<TemporalGraph> operator) {
    return operator.execute(this);
  }

  @Override
  public TemporalGraph callForGraph(BinaryBaseGraphToBaseGraphOperator<TemporalGraph> operator,
    TemporalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  @Override
  public TemporalGraphCollection callForCollection(
    UnaryBaseGraphToBaseGraphCollectionOperator<TemporalGraph, TemporalGraphCollection> operator) {
    return operator.execute(this);
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
   * Convenience API function to create a {@link TemporalGraph} from an existing {@link LogicalGraph} with
   * default values for the temporal attributes.
   *
   * @param logicalGraph the existing logical graph instance
   * @return a temporal graph with default temporal values
   * @see TemporalGraphFactory#fromNonTemporalGraph(BaseGraph)
   */
  public static TemporalGraph fromLogicalGraph(LogicalGraph logicalGraph) {
    return TemporalGradoopConfig.fromGradoopFlinkConfig(logicalGraph.getConfig()).getTemporalGraphFactory()
      .fromNonTemporalGraph(logicalGraph);
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
