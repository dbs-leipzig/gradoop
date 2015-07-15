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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.Aggregation;
import org.gradoop.model.impl.operators.Combination;
import org.gradoop.model.impl.operators.Exclusion;
import org.gradoop.model.impl.operators.Overlap;
import org.gradoop.model.impl.operators.Summarization;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.operators.EPGraphOperators;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import java.util.List;
import java.util.Map;

/**
 * Represents a single graph inside the EPGM. Holds information about the
 * graph (label, properties) and offers unary, binary and auxiliary operators.
 *
 * @author Martin Junghanns
 * @author Niklas Teichmann
 */
public class EPGraph implements EPGraphData, EPGraphOperators {

  /* Convenience key selectors */

  public static final KeySelector<Subgraph<Long, EPFlinkGraphData>, Long>
    GRAPH_ID;
  public static final KeySelector<Vertex<Long, EPFlinkVertexData>, Long>
    VERTEX_ID;
  public static final KeySelector<Edge<Long, EPFlinkEdgeData>, Long> EDGE_ID;
  public static final KeySelector<Edge<Long, EPFlinkEdgeData>, Long>
    SOURCE_VERTEX_ID;
  public static final KeySelector<Edge<Long, EPFlinkEdgeData>, Long>
    TARGET_VERTEX_ID;

  static {
    GRAPH_ID = new GraphKeySelector();
    VERTEX_ID = new VertexKeySelector();
    EDGE_ID = new EdgeKeySelector();
    SOURCE_VERTEX_ID = new EdgeSourceVertexKeySelector();
    TARGET_VERTEX_ID = new EdgeTargetVertexKeySelector();
  }

  /**
   * Gelly graph that encapsulates the vertex and edge datasets associated
   * with that EPGraph.
   */
  private Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph;
  /**
   * Graph payload.
   */
  private EPFlinkGraphData graphData;

  private EPGraph(Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    EPFlinkGraphData graphData) {
    this.graph = graph;
    this.graphData = graphData;
  }

  public static EPGraph fromGraph(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    EPFlinkGraphData graphData) {
    return new EPGraph(graph, graphData);
  }

  public Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> getGellyGraph() {
    return this.graph;
  }

  @Override
  public EPVertexCollection getVertices() {
    return new EPVertexCollection(graph.getVertices());
  }

  @Override
  public EPEdgeCollection getEdges() {
    return new EPEdgeCollection(graph.getEdges());
  }

  @Override
  public EPEdgeCollection getOutgoingEdges(final Long vertexID) {
    DataSet<Edge<Long, EPFlinkEdgeData>> outgoingEdges = this.graph.getEdges()
      .filter(new FilterFunction<Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public boolean filter(Edge<Long, EPFlinkEdgeData> edgeTuple) throws
          Exception {
          return edgeTuple.getValue().getSourceVertex().equals(vertexID);
        }
      });
    return new EPEdgeCollection(outgoingEdges);
  }

  @Override
  public EPEdgeCollection getIncomingEdges(final Long vertexID) {
    return new EPEdgeCollection(this.graph.getEdges()
      .filter(new FilterFunction<Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public boolean filter(Edge<Long, EPFlinkEdgeData> edgeTuple) throws
          Exception {
          return edgeTuple.getValue().getTargetVertex().equals(vertexID);
        }
      }));
  }

  @Override
  public long getVertexCount() throws Exception {
    return this.graph.numberOfVertices();
  }

  @Override
  public long getEdgeCount() throws Exception {
    return this.graph.numberOfEdges();
  }

  @Override
  public org.gradoop.model.impl.EPGraphCollection match(String graphPattern,
    Predicate<EPPatternGraph> predicateFunc) {
    return null;
  }

  @Override
  public EPGraph project(
    UnaryFunction<EPVertexData, EPVertexData> vertexFunction,
    UnaryFunction<EPEdgeData, EPEdgeData> edgeFunction) {
    return null;
  }

  @Override
  public <O extends Number> EPGraph aggregate(String propertyKey,
    UnaryFunction<EPGraph, O> aggregateFunc) throws Exception {
    return callForGraph(new Aggregation<>(propertyKey, aggregateFunc));
  }

  @Override
  public <O1 extends Number, O2 extends Number> EPGraph summarize(
    List<String> vertexGroupingKeys,
    UnaryFunction<Iterable<EPVertexData>, O1> vertexAggregateFunc,
    List<String> edgeGroupingKeys,
    UnaryFunction<Iterable<EPEdgeData>, O2> edgeAggregateFunc) throws
    Exception {
    return null;
  }

  @Override
  public EPGraph summarize(final String vertexGroupingKey) throws Exception {
    Summarization summarization = new Summarization.SummarizationBuilder()
      .vertexGroupingKey(vertexGroupingKey).build();
    return callForGraph(summarization);
  }

  @Override
  public EPGraph summarize(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception {
    Summarization summarization = new Summarization.SummarizationBuilder()
      .vertexGroupingKey(vertexGroupingKey).edgeGroupingKey(edgeGroupingKey)
      .build();
    return callForGraph(summarization);
  }

  @Override
  public EPGraph summarize(String vertexGroupingKey,
    UnaryFunction<Iterable<EPVertexData>, Number> vertexAggregateFunc,
    String edgeGroupingKey,
    UnaryFunction<Iterable<EPEdgeData>, Number> edgeAggregateFunc) throws
    Exception {
    return null;
  }

  @Override
  public EPGraph combine(EPGraph otherGraph) {
    return callForGraph(new Combination(), otherGraph);
  }

  @Override
  public EPGraph overlap(EPGraph otherGraph) {
    return callForGraph(new Overlap(), otherGraph);
  }

  @Override
  public EPGraph exclude(EPGraph otherGraph) {
    return callForGraph(new Exclusion(), otherGraph);
  }

  @Override
  public EPGraph callForGraph(UnaryGraphToGraphOperator operator) throws
    Exception {
    return operator.execute(this);
  }

  @Override
  public EPGraph callForGraph(BinaryGraphToGraphOperator operator,
    EPGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  @Override
  public EPGraphCollection callForCollection(
    UnaryGraphToCollectionOperator operator) {
    return operator.execute(this);
  }

  @Override
  public Map<String, Object> getProperties() {
    return graphData.getProperties();
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return graphData.getPropertyKeys();
  }

  @Override
  public Object getProperty(String key) {
    return graphData.getProperty(key);
  }

  @Override
  public <T> T getProperty(String key, Class<T> type) {
    return graphData.getProperty(key, type);
  }

  @Override
  public void setProperty(String key, Object value) {
    graphData.setProperty(key, value);
  }

  @Override
  public int getPropertyCount() {
    return graphData.getPropertyCount();
  }

  @Override
  public Long getId() {
    return graphData.getId();
  }

  @Override
  public void setId(Long id) {
    graphData.setId(id);
  }

  @Override
  public String getLabel() {
    return graphData.getLabel();
  }

  @Override
  public void setLabel(String label) {
    graphData.setLabel(label);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(String.format("EPGraph{%n"));
    sb.append(
      String.format("%-10d %s %n", graphData.getId(), graphData.toString()));
    try {
      sb.append(String.format("Vertices:%n"));
      for (EPVertexData v : this.getVertices().collect()) {
        sb.append(String.format("%-10d %s %n", v.getId(), v));
      }
      sb.append(String.format("Edges:%n"));
      for (EPEdgeData e : this.getEdges().collect()) {
        sb.append(String.format("%-10d %s %n", e.getId(), e));
      }
      sb.append('}');
    } catch (Exception e) {
      sb.append(e);
    }
    return sb.toString();
  }

  /**
   * Returns the unique graph identifer.
   */
  private static class GraphKeySelector implements
    KeySelector<Subgraph<Long, EPFlinkGraphData>, Long> {
    @Override
    public Long getKey(Subgraph<Long, EPFlinkGraphData> g) throws Exception {
      return g.f0;
    }
  }

  /**
   * Used for distinction of vertices based on their unique id.
   */
  private static class VertexKeySelector implements
    KeySelector<Vertex<Long, EPFlinkVertexData>, Long> {
    @Override
    public Long getKey(
      Vertex<Long, EPFlinkVertexData> longEPFlinkVertexDataVertex) throws
      Exception {
      return longEPFlinkVertexDataVertex.f0;
    }
  }

  /**
   * Used for distinction of edges based on their unique id.
   */
  private static class EdgeKeySelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(
      Edge<Long, EPFlinkEdgeData> longEPFlinkEdgeDataEdge) throws Exception {
      return longEPFlinkEdgeDataEdge.f2.getId();
    }
  }

  /**
   * Used to select the source vertex id of an edge.
   */
  private static class EdgeSourceVertexKeySelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(Edge<Long, EPFlinkEdgeData> e) throws Exception {
      return e.getSource();
    }
  }

  /**
   * Used to select the target vertex id of an edge.
   */
  private static class EdgeTargetVertexKeySelector implements
    KeySelector<Edge<Long, EPFlinkEdgeData>, Long> {
    @Override
    public Long getKey(Edge<Long, EPFlinkEdgeData> e) throws Exception {
      return e.getTarget();
    }
  }
}
