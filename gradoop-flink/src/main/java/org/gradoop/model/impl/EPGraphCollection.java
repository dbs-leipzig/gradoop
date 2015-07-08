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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.operators.EPGraphCollectionOperators;

import java.util.Collection;

public class EPGraphCollection implements
  EPGraphCollectionOperators<EPGraphData> {

  private ExecutionEnvironment env;

  private Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph;

  private DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs;

  public EPGraphCollection(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph,
    DataSet<Subgraph<Long, EPFlinkGraphData>> subgraphs,
    ExecutionEnvironment env) {
    this.graph = graph;
    this.subgraphs = subgraphs;
    this.env = env;
  }

  @Override
  public EPGraph getGraph(final Long graphID) throws Exception {

    // filter vertices and edges based on given graph id
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> subGraph = this.graph
      .subgraph(new VertexGraphContainmentFilter(graphID),
        new EdgeGraphContainmentFilter(graphID));

    // get graph data based on graph id
    EPFlinkGraphData graphData =
      subgraphs.filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {
        @Override
        public boolean filter(Subgraph<Long, EPFlinkGraphData> graph) throws
          Exception {
          return graph.getId().equals(graphID);
        }
      }).collect().get(0).getValue();

    return EPGraph.fromGraph(subGraph, graphData, env);
  }

  @Override
  public EPGraphCollection filter(
    final Predicate<EPGraphData> predicateFunction) throws Exception {
    // find subgraphs matching the predicate
    DataSet<Subgraph<Long, EPFlinkGraphData>> filteredSubgraphs = this.subgraphs
      .filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {

        @Override
        public boolean filter(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return predicateFunction
            .filter(longEPFlinkGraphDataSubgraph.getValue());
        }
      });

    // get the identifiers of these subgraphs
    final Collection<Long> graphIDs = filteredSubgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, Long>() {

        @Override
        public Long map(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return longEPFlinkGraphDataSubgraph.getId();
        }
      }).collect();

    // use graph ids to filter vertices from the actual graph structure
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> filteredGraph =
      this.graph.filterOnVertices(

        new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {
          @Override
          public boolean filter(
            Vertex<Long, EPFlinkVertexData> longEPFlinkVertexDataVertex) throws
            Exception {
            for (Long graphID : longEPFlinkVertexDataVertex.getValue()
              .getGraphs()) {
              if (graphIDs.contains(graphID)) {
                return true;
              }
            }
            return false;
          }
        });

    // create new graph
    return new EPGraphCollection(filteredGraph, filteredSubgraphs, env);
  }

  @Override
  public EPGraphCollection select(Predicate<EPGraph> predicateFunction) throws
    Exception {
    return null;
  }

  @Override
  public EPGraphCollection union(EPGraphCollection otherCollection) {
    return null;
  }

  @Override
  public EPGraphCollection intersect(EPGraphCollection otherCollection) {
    return null;
  }

  @Override
  public EPGraphCollection difference(EPGraphCollection otherCollection) {
    return null;
  }

  @Override
  public EPGraphCollection distinct() {
    return null;
  }

  @Override
  public EPGraphCollection sortBy(String propertyKey, Order order) {
    return null;
  }

  @Override
  public EPGraphCollection top(int limit) {
    return null;
  }

  @Override
  public EPGraphCollection apply(UnaryFunction unaryFunction) {
    return null;
  }

  @Override
  public EPGraph reduce(BinaryFunction binaryGraphOperator) {
    return null;
  }

  @Override
  public EPGraph callForGraph(Algorithm algorithm, String... params) {
    return null;
  }

  @Override
  public EPGraphCollection callForCollection(Algorithm algorithm,
    String... params) {
    return null;
  }

  @Override
  public <V> Iterable<V> values(Class<V> propertyType, String propertyKey) {
    return null;
  }

  @Override
  public Collection<EPGraphData> collect() throws Exception {
    return this.subgraphs
      .map(new MapFunction<Subgraph<Long, EPFlinkGraphData>, EPGraphData>() {
        @Override
        public EPFlinkGraphData map(
          Subgraph<Long, EPFlinkGraphData> longEPFlinkGraphDataSubgraph) throws
          Exception {
          return longEPFlinkGraphDataSubgraph.getValue();
        }
      }).collect();
  }

  @Override
  public long size() throws Exception {
    return subgraphs.count();
  }

  @Override
  public void print() throws Exception {
    subgraphs.print();
  }

  EPGraph getGraph() {
    return EPGraph.fromGraph(this.graph, null, env);
  }

  private static class VertexGraphContainmentFilter implements
    FilterFunction<Vertex<Long, EPFlinkVertexData>> {

    private long graphID;

    public VertexGraphContainmentFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Vertex<Long, EPFlinkVertexData> v) throws Exception {
      return v.f1.getGraphs().contains(graphID);
    }
  }

  private static class EdgeGraphContainmentFilter implements
    FilterFunction<Edge<Long, EPFlinkEdgeData>> {

    private long graphID;

    public EdgeGraphContainmentFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Edge<Long, EPFlinkEdgeData> e) throws Exception {
      return e.f2.getGraphs().contains(graphID);
    }
  }
}
