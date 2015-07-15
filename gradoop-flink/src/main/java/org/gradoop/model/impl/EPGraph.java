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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.operators.Combination;
import org.gradoop.model.impl.operators.Summarization;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.operators.EPGraphOperators;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import java.util.Iterator;
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
   * Flink execution environment.
   */
  private ExecutionEnvironment env;

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
    EPFlinkGraphData graphData, ExecutionEnvironment env) {
    this.graph = graph;
    this.graphData = graphData;
    this.env = env;
  }

  public static EPGraph fromGraph(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    EPFlinkGraphData graphData, ExecutionEnvironment env) {
    return new EPGraph(graph, graphData, env);
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
    DataSet<Edge<Long, EPFlinkEdgeData>> outgoingEdges =
      this.graph.getEdges().filter(

        new FilterFunction<Edge<Long, EPFlinkEdgeData>>() {
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
    O result = aggregateFunc.execute(this);
    // copy graph data before updating properties
    EPFlinkGraphData newGraphData = new EPFlinkGraphData(this.graphData);
    newGraphData.setProperty(propertyKey, result);
    return EPGraph.fromGraph(this.graph, newGraphData, env);
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
    final Long newGraphID = FlinkConstants.OVERLAP_GRAPH_ID;

    // union vertex sets, group by vertex id, filter vertices where
    // the group contains two vertices and update them with the new graph id
    DataSet<Vertex<Long, EPFlinkVertexData>> newVertexSet =
      this.graph.getVertices().union(otherGraph.graph.getVertices())
        .groupBy(VERTEX_ID).reduceGroup(new VertexGroupReducer(2L))
        .map(new VertexToGraphUpdater(newGraphID));

    DataSet<Edge<Long, EPFlinkEdgeData>> newEdgeSet =
      this.graph.getEdges().union(otherGraph.graph.getEdges()).groupBy(EDGE_ID)
        .reduceGroup(new EdgeGroupReducer(2L))
        .map(new EdgeToGraphUpdater(newGraphID));

    return EPGraph.fromGraph(Graph.fromDataSet(newVertexSet, newEdgeSet, env),
      new EPFlinkGraphData(newGraphID, FlinkConstants.DEFAULT_GRAPH_LABEL),
      env);
  }

  @Override
  public EPGraph exclude(EPGraph otherGraph) {
    final Long newGraphID = FlinkConstants.EXCLUDE_GRAPH_ID;

    // union vertex sets, group by vertex id, filter vertices where the group
    // contains exactly one vertex which belongs to the graph, the operator is
    // called on
    DataSet<Vertex<Long, EPFlinkVertexData>> newVertexSet =
      this.graph.getVertices().union(otherGraph.graph.getVertices())
        .groupBy(VERTEX_ID).reduceGroup(
        new VertexGroupReducer(1L, this.getId(), otherGraph.getId()))
        .map(new VertexToGraphUpdater(newGraphID));

    JoinFunction<Edge<Long, EPFlinkEdgeData>, Vertex<Long,
      EPFlinkVertexData>, Edge<Long, EPFlinkEdgeData>>
      joinFunc =
      new JoinFunction<Edge<Long, EPFlinkEdgeData>, Vertex<Long,
        EPFlinkVertexData>, Edge<Long, EPFlinkEdgeData>>() {
        @Override
        public Edge<Long, EPFlinkEdgeData> join(
          Edge<Long, EPFlinkEdgeData> leftTuple,
          Vertex<Long, EPFlinkVertexData> rightTuple) throws Exception {
          return leftTuple;
        }
      };

    // In exclude(), we are only interested in edges that connect vertices
    // that are in the exclusion of the vertex sets. Thus, we join the edges
    // from the left graph with the new vertex set using source and target ids.
    DataSet<Edge<Long, EPFlinkEdgeData>> newEdgeSet =
      this.graph.getEdges().join(newVertexSet).where(SOURCE_VERTEX_ID)
        .equalTo(VERTEX_ID).with(joinFunc).join(newVertexSet)
        .where(TARGET_VERTEX_ID).equalTo(VERTEX_ID).with(joinFunc)
        .map(new EdgeToGraphUpdater(newGraphID));

    return EPGraph.fromGraph(Graph.fromDataSet(newVertexSet, newEdgeSet, env),
      new EPFlinkGraphData(newGraphID, FlinkConstants.DEFAULT_GRAPH_LABEL),
      env);
  }

  @Override
  public EPGraph callForGraph(UnaryGraphToGraphOperator operator) {
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

  /**
   * Used for {@code EPGraph.overlap()} and {@code EPGraph.exclude()}
   * <p>
   * Checks if the number of grouped, duplicate vertices is equal to a
   * given amount. If yes, reducer returns the vertex.
   * <p>
   * Furthermore, to realize exclusion, if two graphs are given, the method
   * checks if the vertex is contained in the first (include graph) but not
   * in the other graph (preclude graph). If this is the case, the vertex
   * gets returned.
   */
  private static class VertexGroupReducer implements
    GroupReduceFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
      EPFlinkVertexData>> {

    /**
     * number of times a vertex must occur inside a group
     */
    private long amount;

    /**
     * graph, a vertex must be part of
     */
    private Long includedGraphID;

    /**
     * graph, a vertex must not be part of
     */
    private Long precludedGraphID;

    public VertexGroupReducer(long amount) {
      this(amount, null, null);
    }

    public VertexGroupReducer(long amount, Long includedGraphID,
      Long precludedGraphID) {
      this.amount = amount;
      this.includedGraphID = includedGraphID;
      this.precludedGraphID = precludedGraphID;
    }

    @Override
    public void reduce(Iterable<Vertex<Long, EPFlinkVertexData>> iterable,
      Collector<Vertex<Long, EPFlinkVertexData>> collector) throws Exception {
      Iterator<Vertex<Long, EPFlinkVertexData>> iterator = iterable.iterator();
      long count = 0L;
      Vertex<Long, EPFlinkVertexData> v = null;
      while (iterator.hasNext()) {
        v = iterator.next();
        count++;
      }
      if (count == amount) {
        if (includedGraphID != null && precludedGraphID != null) {
          assert v != null;
          if (v.getValue().getGraphs().contains(includedGraphID) &&
            !v.getValue().getGraphs().contains(precludedGraphID)) {
            collector.collect(v);
          }
        } else {
          collector.collect(v);
        }
      }
    }
  }

  /**
   * Used for {@code EPGraph.overlap()} and {@code EPGraph.exclude()}
   * <p>
   * Used to check if the number of grouped, duplicate edges is equal to a
   * given amount. If yes, reducer returns the vertex.
   */
  private static class EdgeGroupReducer implements
    GroupReduceFunction<Edge<Long, EPFlinkEdgeData>, Edge<Long,
      EPFlinkEdgeData>> {

    private long amount;

    public EdgeGroupReducer(long amount) {
      this.amount = amount;
    }

    @Override
    public void reduce(Iterable<Edge<Long, EPFlinkEdgeData>> iterable,
      Collector<Edge<Long, EPFlinkEdgeData>> collector) throws Exception {
      Iterator<Edge<Long, EPFlinkEdgeData>> iterator = iterable.iterator();
      long count = 0L;
      Edge<Long, EPFlinkEdgeData> e = null;
      while (iterator.hasNext()) {
        e = iterator.next();
        count++;
      }
      if (count == amount) {
        collector.collect(e);
      }
    }
  }

  /**
   * Adds a given graph ID to the vertex and returns it.
   */
  public static class VertexToGraphUpdater implements
    MapFunction<Vertex<Long, EPFlinkVertexData>, Vertex<Long,
      EPFlinkVertexData>> {

    private final long newGraphID;

    public VertexToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    @Override
    public Vertex<Long, EPFlinkVertexData> map(
      Vertex<Long, EPFlinkVertexData> v) throws Exception {
      v.getValue().addGraph(newGraphID);
      return v;
    }
  }

  /**
   * Adds a given graph ID to the edge and returns it.
   */
  public static class EdgeToGraphUpdater implements
    MapFunction<Edge<Long, EPFlinkEdgeData>, Edge<Long, EPFlinkEdgeData>> {

    private final long newGraphID;

    public EdgeToGraphUpdater(final long newGraphID) {
      this.newGraphID = newGraphID;
    }

    @Override
    public Edge<Long, EPFlinkEdgeData> map(Edge<Long, EPFlinkEdgeData> e) throws
      Exception {
      e.getValue().addGraph(newGraphID);
      return e;
    }
  }
}
