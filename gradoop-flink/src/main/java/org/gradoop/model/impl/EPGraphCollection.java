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
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.EdgeData;
import org.gradoop.model.EdgeDataFactory;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.VertexDataFactory;
import org.gradoop.model.helper.KeySelectors;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 *
 * @author Martin Junghanns
 * @author Niklas Teichmann
 */
public class EPGraphCollection<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  EPGraphCollectionOperators<VD, ED, GD> {

  private final VertexDataFactory<VD> vertexDataFactory;

  private final EdgeDataFactory<ED> edgeDataFactory;

  private final GraphDataFactory<GD> graphDataFactory;

  private ExecutionEnvironment env;

  private Graph<Long, VD, ED> graph;

  private DataSet<Subgraph<Long, GD>> subgraphs;

  public EPGraphCollection(Graph<Long, VD, ED> graph,
    DataSet<Subgraph<Long, GD>> subgraphs,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.graph = graph;
    this.subgraphs = subgraphs;
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.env = env;
  }

  public VertexDataFactory<VD> getVertexDataFactory() {
    return vertexDataFactory;
  }

  public EdgeDataFactory<ED> getEdgeDataFactory() {
    return edgeDataFactory;
  }

  public GraphDataFactory<GD> getGraphDataFactory() {
    return graphDataFactory;
  }

  public Graph<Long, VD, ED> getGellyGraph() {
    return this.graph;
  }

  public DataSet<Subgraph<Long, GD>> getSubgraphs() {
    return this.subgraphs;
  }

  @Override
  public EPGraph<VD, ED, GD> getGraph(final Long graphID) throws Exception {
    // filter vertices and edges based on given graph id
    Graph<Long, VD, ED> subGraph = this.graph
      .subgraph(new VertexInGraphFilter<VD>(graphID),
        new EdgeInGraphFilter<ED>(graphID));

    DataSet<Tuple1<Long>> graphIDDataSet =
      env.fromCollection(Lists.newArrayList(new Tuple1<>(graphID)));

    // get graph data based on graph id
    GD graphData = this.subgraphs.joinWithTiny(graphIDDataSet)
      .where(new KeySelectors.GraphKeySelector<GD>()).equalTo(0)
      .with(new JoinFunction<Subgraph<Long, GD>, Tuple1<Long>, GD>() {
        @Override
        public GD join(Subgraph<Long, GD> g, Tuple1<Long> gID) throws
          Exception {
          return g.getValue();
        }
      }).first(1).collect().get(0);
    return EPGraph
      .fromGraph(subGraph, graphData, vertexDataFactory, edgeDataFactory,
        graphDataFactory);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> getGraphs(
    final Long... identifiers) throws Exception {
    return getGraphs(Arrays.asList(identifiers));
  }

  @Override
  public EPGraphCollection<VD, ED, GD> getGraphs(
    final List<Long> identifiers) throws Exception {

    DataSet<Subgraph<Long, GD>> newSubGraphs =
      this.subgraphs.filter(new FilterFunction<Subgraph<Long, GD>>() {

        @Override
        public boolean filter(Subgraph<Long, GD> subgraph) throws Exception {
          return identifiers.contains(subgraph.getId());

        }
      });

    // build new vertex set
    DataSet<Vertex<Long, VD>> vertices = this.graph.getVertices()
      .filter(new VertexInGraphsFilter<VD>(identifiers));

    // build new edge set
    DataSet<Edge<Long, ED>> edges =
      this.graph.getEdges().filter(new EdgeInGraphsFilter<ED>(identifiers));

    return new EPGraphCollection<>(Graph.fromDataSet(vertices, edges, env),
      newSubGraphs, vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  @Override
  public long getGraphCount() throws Exception {
    return this.subgraphs.count();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> filter(
    final Predicate<GD> predicateFunction) throws Exception {
    // find subgraphs matching the predicate
    DataSet<Subgraph<Long, GD>> filteredSubgraphs =
      this.subgraphs.filter(new FilterFunction<Subgraph<Long, GD>>() {
        @Override
        public boolean filter(Subgraph<Long, GD> g) throws Exception {
          return predicateFunction.filter(g.getValue());
        }
      });

    // get the identifiers of these subgraphs
    final Collection<Long> graphIDs =
      filteredSubgraphs.map(new MapFunction<Subgraph<Long, GD>, Long>() {

        @Override
        public Long map(Subgraph<Long, GD> g) throws Exception {
          return g.getId();
        }
      }).collect();

    // use graph ids to filter vertices from the actual graph structure
    Graph<Long, VD, ED> filteredGraph = this.graph.filterOnVertices(

      new FilterFunction<Vertex<Long, VD>>() {
        @Override
        public boolean filter(Vertex<Long, VD> v) throws Exception {
          for (Long graphID : v.getValue().getGraphs()) {
            if (graphIDs.contains(graphID)) {
              return true;
            }
          }
          return false;
        }
      });

    return new EPGraphCollection<>(filteredGraph, filteredSubgraphs,
      vertexDataFactory, edgeDataFactory, graphDataFactory, env);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> select(
    Predicate<EPGraph<VD, ED, GD>> predicateFunction) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> union(
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Union<VD, ED, GD>(), otherCollection);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> intersect(
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Intersect<VD, ED, GD>(), otherCollection);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> intersectWithSmall(
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new IntersectWithSmall<VD, ED, GD>(),
      otherCollection);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> difference(
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Difference<VD, ED, GD>(), otherCollection);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> differenceWithSmallResult(
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new DifferenceWithSmallResult<VD, ED, GD>(),
      otherCollection);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> distinct() {
    throw new NotImplementedException();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> top(int limit) {
    throw new NotImplementedException();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> apply(
    UnaryGraphToGraphOperator<VD, ED, GD> op) {
    throw new NotImplementedException();
  }

  @Override
  public EPGraph<VD, ED, GD> reduce(BinaryGraphToGraphOperator<VD, ED, GD> op) {
    throw new NotImplementedException();
  }

  @Override
  public EPGraphCollection<VD, ED, GD> callForCollection(
    UnaryCollectionToCollectionOperator<VD, ED, GD> op) {
    return op.execute(this);
  }

  @Override
  public EPGraphCollection<VD, ED, GD> callForCollection(
    BinaryCollectionToCollectionOperator<VD, ED, GD> op,
    EPGraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return op.execute(this, otherCollection);
  }

  @Override
  public EPGraph<VD, ED, GD> callForGraph(
    UnaryCollectionToGraphOperator<VD, ED, GD> op) {
    return op.execute(this);
  }

  @Override
  public void writeAsJson(String vertexFile, String edgeFile,
    String graphFile) throws Exception {
    this.getGellyGraph().getVertices().writeAsFormattedText(vertexFile,
      new JsonWriter.VertexTextFormatter<VD>()).getDataSet().collect();
    this.getGellyGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<ED>())
      .getDataSet().collect();
    this.getSubgraphs()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<GD>())
      .getDataSet().collect();
  }

  @Override
  public <V> Iterable<V> values(Class<V> propertyType, String propertyKey) {
    throw new NotImplementedException();
  }

  @Override
  public Collection<GD> collect() throws Exception {
    return this.subgraphs.map(new MapFunction<Subgraph<Long, GD>, GD>() {
      @Override
      public GD map(Subgraph<Long, GD> g) throws Exception {
        return g.getValue();
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

  EPGraph<VD, ED, GD> getGraph() {
    return EPGraph
      .fromGraph(this.graph, null, vertexDataFactory, edgeDataFactory,
        graphDataFactory);
  }


  private static class VertexInGraphFilter<VD extends VertexData> implements
    FilterFunction<Vertex<Long, VD>> {

    private long graphID;

    public VertexInGraphFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Vertex<Long, VD> v) throws Exception {
      return (v.getValue().getGraphCount() > 0) &&
        v.getValue().getGraphs().contains(graphID);
    }
  }

  private static class EdgeInGraphFilter<ED extends EdgeData> implements
    FilterFunction<Edge<Long, ED>> {

    private long graphID;

    public EdgeInGraphFilter(long graphID) {
      this.graphID = graphID;
    }

    @Override
    public boolean filter(Edge<Long, ED> e) throws Exception {
      return (e.getValue().getGraphCount() > 0) &&
        e.getValue().getGraphs().contains(graphID);
    }
  }

  private static class VertexInGraphsFilter<VD extends VertexData> implements
    FilterFunction<Vertex<Long, VD>> {

    List<Long> identifiers;

    public VertexInGraphsFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Vertex<Long, VD> vertex) throws Exception {
      boolean vertexInGraph = false;
      if (vertex.getValue().getGraphCount() > 0) {
        for (Long graph : vertex.getValue().getGraphs()) {
          if (identifiers.contains(graph)) {
            vertexInGraph = true;
            break;
          }
        }
      }
      return vertexInGraph;
    }
  }

  private static class EdgeInGraphsFilter<ED extends EdgeData> implements
    FilterFunction<Edge<Long, ED>> {

    List<Long> identifiers;

    public EdgeInGraphsFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    @Override
    public boolean filter(Edge<Long, ED> e) throws Exception {
      boolean vertexInGraph = false;
      if (e.getValue().getGraphCount() > 0) {
        for (Long graph : e.getValue().getGraphs()) {
          if (identifiers.contains(graph)) {
            vertexInGraph = true;
            break;
          }
        }
      }
      return vertexInGraph;
    }
  }
}
