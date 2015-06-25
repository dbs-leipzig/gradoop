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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.operators.EPGraphCollectionOperators;

import java.util.Collection;

public class EPGraphCollection implements EPGraphCollectionOperators {

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
      .filterOnVertices(new FilterFunction<Vertex<Long, EPFlinkVertexData>>() {
        @Override
        public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
          Exception {
          return vertex.getValue().getGraphs().contains(graphID);
        }
      });
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
  public EPGraphCollection select(Predicate<EPGraph> predicateFunction) {
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
}
