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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EPGraphData;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.impl.operators.Difference;
import org.gradoop.model.impl.operators.DifferenceWithSmallResult;
import org.gradoop.model.impl.operators.Intersect;
import org.gradoop.model.impl.operators.IntersectWithSmall;
import org.gradoop.model.impl.operators.Union;
import org.gradoop.model.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.operators.EPGraphCollectionOperators;
import org.gradoop.model.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.gradoop.model.impl.EPGraph.GRAPH_ID;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 *
 * @author Martin Junghanns
 * @author Niklas Teichmann
 */
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

  public Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> getGellyGraph() {
    return this.graph;
  }

  public DataSet<Subgraph<Long, EPFlinkGraphData>> getSubgraphs() {
    return this.subgraphs;
  }

  @Override
  public EPGraph getGraph(final Long graphID) throws Exception {
    // filter vertices and edges based on given graph id
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> subGraph = this.graph
      .subgraph(new VertexGraphContainmentFilter(graphID),
        new EdgeGraphContainmentFilter(graphID));

    DataSet<Tuple1<Long>> graphIDDataSet =
      env.fromCollection(Lists.newArrayList(new Tuple1<>(graphID)));

    // get graph data based on graph id
    EPFlinkGraphData graphData =
      this.subgraphs.joinWithTiny(graphIDDataSet).where(GRAPH_ID).equalTo(0)
        .with(
          new JoinFunction<Subgraph<Long, EPFlinkGraphData>, Tuple1<Long>,
            EPFlinkGraphData>() {
            @Override
            public EPFlinkGraphData join(Subgraph<Long, EPFlinkGraphData> g,
              Tuple1<Long> gID) throws Exception {
              return g.getValue();
            }
          }).first(1).collect().get(0);
    return EPGraph.fromGraph(subGraph, graphData);
  }

  @Override
  public EPGraphCollection getGraphs(final Long... identifiers) throws
    Exception {
    return getGraphs(Arrays.asList(identifiers));
  }

  @Override
  public EPGraphCollection getGraphs(final List<Long> identifiers) throws
    Exception {

    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubGraphs = this.subgraphs
      .filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {

        @Override
        public boolean filter(Subgraph<Long, EPFlinkGraphData> subgraph) throws
          Exception {
          return identifiers.contains(subgraph.getId());

        }
      });

    // build new vertex set
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      this.graph.getVertices().filter(new VertexInGraphFilter(identifiers));

    // build new edge set
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      this.graph.getEdges().filter(new EdgeInGraphFilter(identifiers));

    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubGraphs, env);
  }

  @Override
  public Long getGraphCount() throws Exception {
    return this.subgraphs.count();
  }

  @Override
  public EPGraphCollection filter(
    final Predicate<EPGraphData> predicateFunction) throws Exception {
    // find subgraphs matching the predicate
    DataSet<Subgraph<Long, EPFlinkGraphData>> filteredSubgraphs = this.subgraphs
      .filter(new FilterFunction<Subgraph<Long, EPFlinkGraphData>>() {
        @Override
        public boolean filter(Subgraph<Long, EPFlinkGraphData> g) throws
          Exception {
          return predicateFunction.filter(g.getValue());
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

    return new EPGraphCollection(filteredGraph, filteredSubgraphs, env);
  }

  @Override
  public EPGraphCollection select(Predicate<EPGraph> predicateFunction) throws
    Exception {
    return null;
  }

  @Override
  public EPGraphCollection union(EPGraphCollection otherCollection) throws
    Exception {
    return callForCollection(new Union(), otherCollection);
  }

  @Override
  public EPGraphCollection intersect(EPGraphCollection otherCollection) throws
    Exception {
    return callForCollection(new Intersect(), otherCollection);
  }

  @Override
  public EPGraphCollection intersectWithSmall(
    EPGraphCollection otherCollection) throws Exception {
    return callForCollection(new IntersectWithSmall(), otherCollection);
  }

  @Override
  public EPGraphCollection difference(EPGraphCollection otherCollection) throws
    Exception {
    return callForCollection(new Difference(), otherCollection);
  }

  @Override
  public EPGraphCollection differenceWithSmallResult(
    EPGraphCollection otherCollection) throws Exception {
    return callForCollection(new DifferenceWithSmallResult(), otherCollection);
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
  public EPGraphCollection apply(UnaryGraphToGraphOperator op) {
    return null;
  }

  @Override
  public EPGraph reduce(BinaryGraphToGraphOperator op) {
    return null;
  }

  @Override
  public EPGraphCollection callForCollection(
    UnaryCollectionToCollectionOperator op) {
    return op.execute(this);
  }

  @Override
  public EPGraphCollection callForCollection(
    BinaryCollectionToCollectionOperator op,
    EPGraphCollection otherCollection) throws Exception {
    return op.execute(this, otherCollection);
  }

  @Override
  public EPGraph callForGraph(UnaryCollectionToGraphOperator op) {
    return op.execute(this);
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
    return EPGraph.fromGraph(this.graph, null);
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

  private static class VertexInGraphFilter implements
    FilterFunction<Vertex<Long, EPFlinkVertexData>> {

    List<Long> identifiers;

    public VertexInGraphFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Vertex<Long, EPFlinkVertexData> vertex) throws
      Exception {
      boolean vertexInGraph = false;
      for (Long graph : vertex.getValue().getGraphs()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
      return vertexInGraph;
    }
  }

  private static class EdgeInGraphFilter implements
    FilterFunction<Edge<Long, EPFlinkEdgeData>> {

    List<Long> identifiers;

    public EdgeInGraphFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Edge<Long, EPFlinkEdgeData> vertex) throws Exception {
      boolean vertexInGraph = false;
      for (Long graph : vertex.getValue().getGraphs()) {
        if (identifiers.contains(graph)) {
          vertexInGraph = true;
          break;
        }
      }
      return vertexInGraph;
    }
  }
}
