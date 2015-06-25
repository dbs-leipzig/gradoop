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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPEdgeData;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.EPPatternGraph;
import org.gradoop.model.EPVertexData;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.operators.EPGraphCollectionOperators;
import org.gradoop.model.operators.EPGraphOperators;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a single graph inside the EPGM. Holds information about the
 * graph (label, properties) and offers unary, binary and auxiliary operators.
 */
public class EPGraph implements EPGraphData, EPGraphOperators {

  private ExecutionEnvironment env;

  private Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph;

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
  public EPGraphCollectionOperators match(String graphPattern,
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
    Aggregate<EPGraph, O> aggregateFunc) {
    return null;
  }

  @Override
  public EPGraph summarize(List<String> vertexGroupingKeys,
    Aggregate<Tuple2<EPVertexData, Set<EPVertexData>>, EPVertexData>
      vertexAggregateFunc,
    List<String> edgeGroupingKeys,
    Aggregate<Tuple2<EPEdgeData, Set<EPEdgeData>>, EPEdgeData>
      edgeAggregateFunc) {
    return null;
  }

  @Override
  public EPGraph combine(EPGraph otherGraph) {
    // cannot use Gelly union here because of missing argument for KeySelector
    DataSet<Vertex<Long, EPFlinkVertexData>> newVertexSet =
      this.graph.getVertices().union(otherGraph.graph.getVertices())
        .distinct(new VertexKeySelector());

    DataSet<Edge<Long, EPFlinkEdgeData>> newEdgeSet =
      this.graph.getEdges().union(otherGraph.graph.getEdges())
        .distinct(new EdgeKeySelector());

    return EPGraph.fromGraph(Graph.fromDataSet(newVertexSet, newEdgeSet, env),
      new EPFlinkGraphData(FlinkConstants.COMBINE_GRAPH_ID,
        FlinkConstants.DEFAULT_GRAPH_LABEL), env);
  }

  @Override
  public EPGraph overlap(EPGraph otherGraph) {
    return null;
  }

  @Override
  public EPGraph exclude(EPGraph otherGraph) {
    return null;
  }

  @Override
  public EPGraph callForGraph(Algorithm algorithm, String... params) {
    return null;
  }

  @Override
  public EPGraphCollectionOperators callForCollection(Algorithm algorithm,
    String... params) {
    return null;
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
}
