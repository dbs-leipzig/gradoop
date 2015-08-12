/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.Attributed;
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.Identifiable;
import org.gradoop.model.Labeled;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.Aggregation;
import org.gradoop.model.impl.operators.Combination;
import org.gradoop.model.impl.operators.Exclusion;
import org.gradoop.model.impl.operators.Overlap;
import org.gradoop.model.impl.operators.Projection;
import org.gradoop.model.impl.operators.SummarizationUsingJoin;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.operators.LogicalGraphOperators;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import java.util.Map;

/**
 * Represents a logical graph inside the EPGM. Holds the graph data (label,
 * properties) and offers unary, binary and auxiliary operators.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class LogicalGraph<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  LogicalGraphOperators<VD, ED, GD>, Identifiable, Attributed, Labeled {
  /**
   * Used to create new vertex data.
   */
  private final VertexDataFactory<VD> vertexDataFactory;
  /**
   * Used to create new edge data.
   */
  private final EdgeDataFactory<ED> edgeDataFactory;
  /**
   * Uses to create new graph data.
   */
  private final GraphDataFactory<GD> graphDataFactory;
  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;
  /**
   * Flink Gelly graph that holds the vertex and edge datasets associated
   * with that logical graph.
   */
  private final Graph<Long, VD, ED> graph;
  /**
   * Graph data associated with that logical graph.
   */
  private final GD graphData;

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param graph             flink gelly graph holding vertex and edge set
   * @param graphData         graph data associated with that logical graph
   * @param vertexDataFactory used to create vertex data
   * @param edgeDataFactory   used to create edge data
   * @param graphDataFactory  used to create graph data
   * @param env               Flink execution environment
   */
  private LogicalGraph(Graph<Long, VD, ED> graph, GD graphData,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.graph = graph;
    this.graphData = graphData;
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.env = env;
  }

  /**
   * Returns the vertex data factory.
   *
   * @return vertex data factory
   */
  public VertexDataFactory<VD> getVertexDataFactory() {
    return vertexDataFactory;
  }

  /**
   * Returns the edge data factory.
   *
   * @return edge data factory
   */
  public EdgeDataFactory<ED> getEdgeDataFactory() {
    return edgeDataFactory;
  }

  /**
   * Returns the graph data factory.
   *
   * @return graph data factory
   */
  public GraphDataFactory<GD> getGraphDataFactory() {
    return graphDataFactory;
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param graph             Flink Gelly graph
   * @param graphData         graph data associated with the logical graph
   * @param vertexDataFactory vertex data factory
   * @param edgeDataFactory   edge data factory
   * @param graphDataFactory  graph data factory
   * @param <VD>              vertex data type
   * @param <ED>              edge data type
   * @param <GD>              graph data type
   * @return logical graph
   */
  public static <VD extends VertexData, ED extends EdgeData, GD extends
    GraphData> LogicalGraph<VD, ED, GD> fromGraph(
    Graph<Long, VD, ED> graph, GD graphData,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory,
    GraphDataFactory<GD> graphDataFactory) {
    return new LogicalGraph<>(graph, graphData, vertexDataFactory,
      edgeDataFactory, graphDataFactory, graph.getContext());
  }

  /**
   * Returns the gelly graph which is represented by this logical graph.
   *
   * @return Gelly graph
   */
  public Graph<Long, VD, ED> getGellyGraph() {
    return this.graph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexDataCollection<VD> getVertices() {
    return new VertexDataCollection<>(graph.getVertices());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeDataCollection<ED> getEdges() {
    return new EdgeDataCollection<>(graph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeDataCollection<ED> getOutgoingEdges(final Long vertexID) {
    DataSet<Edge<Long, ED>> outgoingEdges =
      this.graph.getEdges().filter(new FilterFunction<Edge<Long, ED>>() {
        @Override
        public boolean filter(Edge<Long, ED> edgeTuple) throws Exception {
          return edgeTuple.getSource().equals(vertexID);
        }
      });
    return new EdgeDataCollection<>(outgoingEdges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EdgeDataCollection<ED> getIncomingEdges(final Long vertexID) {
    return new EdgeDataCollection<>(
      this.graph.getEdges().filter(new FilterFunction<Edge<Long, ED>>() {
        @Override
        public boolean filter(Edge<Long, ED> edgeTuple) throws Exception {
          return edgeTuple.getTarget().equals(vertexID);
        }
      }));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() throws Exception {
    return this.graph.numberOfVertices();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() throws Exception {
    return this.graph.numberOfEdges();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> match(String graphPattern,
    Predicate<LogicalGraph> predicateFunc) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> project(UnaryFunction<VD, VD> vertexFunction,
    UnaryFunction<ED, ED> edgeFunction) throws Exception {
    return callForGraph(new Projection<VD, ED, GD>(vertexFunction, edgeFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <O extends Number> LogicalGraph<VD, ED, GD> aggregate(
    String propertyKey,
    UnaryFunction<LogicalGraph<VD, ED, GD>, O> aggregateFunc) throws Exception {
    return callForGraph(new Aggregation<>(propertyKey, aggregateFunc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarize(String vertexGroupingKey) throws
    Exception {
    return summarize(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarize(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationUsingJoin<VD, ED, GD>(vertexGroupingKey, edgeGroupingKey,
        false, false));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexLabel() throws Exception {
    return summarizeOnVertexLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception {
    return summarizeOnVertexLabel(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception {
    return summarizeOnVertexLabel(null, edgeGroupingKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexLabel(
    String vertexGroupingKey, String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationUsingJoin<VD, ED, GD>(vertexGroupingKey, edgeGroupingKey,
        true, false));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabel() throws
    Exception {
    return summarizeOnVertexAndEdgeLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD>
  summarizeOnVertexAndEdgeLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception {
    return summarizeOnVertexAndEdgeLabel(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception {
    return summarizeOnVertexAndEdgeLabel(null, edgeGroupingKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> summarizeOnVertexAndEdgeLabel(
    String vertexGroupingKey, String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationUsingJoin<VD, ED, GD>(vertexGroupingKey, edgeGroupingKey,
        true, true));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> combine(LogicalGraph<VD, ED, GD> otherGraph) {
    return callForGraph(new Combination<VD, ED, GD>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> overlap(LogicalGraph<VD, ED, GD> otherGraph) {
    return callForGraph(new Overlap<VD, ED, GD>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> exclude(LogicalGraph<VD, ED, GD> otherGraph) {
    return callForGraph(new Exclusion<VD, ED, GD>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> callForGraph(
    UnaryGraphToGraphOperator<VD, ED, GD> operator) throws Exception {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> callForGraph(
    BinaryGraphToGraphOperator<VD, ED, GD> operator,
    LogicalGraph<VD, ED, GD> otherGraph) {
    return operator.execute(this, otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> callForCollection(
    UnaryGraphToCollectionOperator<VD, ED, GD> operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Object> getProperties() {
    return graphData.getProperties();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getPropertyKeys() {
    return graphData.getPropertyKeys();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getProperty(String key) {
    return graphData.getProperty(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T getProperty(String key, Class<T> type) {
    return graphData.getProperty(key, type);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperties(Map<String, Object> properties) {
    graphData.setProperties(properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, Object value) {
    graphData.setProperty(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPropertyCount() {
    return graphData.getPropertyCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getId() {
    return graphData.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(Long id) {
    graphData.setId(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return graphData.getLabel();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLabel(String label) {
    graphData.setLabel(label);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public void writeAsJson(String vertexFile, String edgeFile,
    String graphFile) throws Exception {
    this.getGellyGraph().getVertices().writeAsFormattedText(vertexFile,
      new JsonWriter.VertexTextFormatter<VD>()).getDataSet().collect();
    this.getGellyGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<ED>())
      .getDataSet().collect();
    env.fromElements(new Subgraph<>(graphData.getId(), graphData))
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<GD>())
      .getDataSet().collect();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String print() throws Exception {
    final StringBuilder sb =
      new StringBuilder(String.format("LogicalGraph{%n"));
    sb.append(
      String.format("%-10d %s %n", graphData.getId(), graphData.toString()));
    sb.append(String.format("Vertices:%n"));
    for (VD v : this.getVertices().collect()) {
      sb.append(String.format("%-10d %s %n", v.getId(), v));
    }
    sb.append(String.format("Edges:%n"));
    for (ED e : this.getEdges().collect()) {
      sb.append(String.format("%-10d %s %n", e.getId(), e));
    }
    sb.append('}');
    return sb.toString();
  }
}
