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
import org.apache.commons.lang.NotImplementedException;
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
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.model.impl.functions.KeySelectors;
import org.gradoop.util.Order;
import org.gradoop.model.impl.functions.Predicate;
import org.gradoop.model.impl.tuples.Subgraph;
import org.gradoop.model.impl.operators.Difference;
import org.gradoop.model.impl.operators.DifferenceUsingList;
import org.gradoop.model.impl.operators.Intersect;
import org.gradoop.model.impl.operators.IntersectUsingList;
import org.gradoop.model.impl.operators.Union;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.api.operators.GraphCollectionOperators;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class GraphCollection<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  extends AbstractGraph<VD, ED, GD>
  implements GraphCollectionOperators<VD, ED, GD> {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private DataSet<Subgraph<Long, GD>> subgraphs;

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param graph             Flink Gelly graph
   * @param subgraphs         graph data associated with logical graphs in that
   *                          collection
   * @param vertexDataFactory vertex data factory
   * @param edgeDataFactory   edge data factory
   * @param graphDataFactory  graph data factory
   * @param env               Flink execution environment
   */
  public GraphCollection(Graph<Long, VD, ED> graph,
    DataSet<Subgraph<Long, GD>> subgraphs,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory, GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    super(graph, vertexDataFactory, edgeDataFactory, graphDataFactory, env);
    this.subgraphs = subgraphs;
  }

  /**
   * Returns the graph data associated with the logical graphs in that
   * collection.
   *
   * @return all logical graph data
   */
  public DataSet<Subgraph<Long, GD>> getSubgraphs() {
    return this.subgraphs;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public LogicalGraph<VD, ED, GD> getGraph(final Long graphID) throws
    Exception {
    // filter vertices and edges based on given graph id
    Graph<Long, VD, ED> subGraph = getGellyGraph()
      .subgraph(new VertexInGraphFilter<VD>(graphID),
        new EdgeInGraphFilter<ED>(graphID));

    DataSet<Tuple1<Long>> graphIDDataSet = getExecutionEnvironment()
      .fromCollection(Lists.newArrayList(new Tuple1<>(graphID)));

    // get graph data based on graph id
    List<GD> graphData = this.subgraphs.joinWithTiny(graphIDDataSet)
      .where(new KeySelectors.GraphKeySelector<GD>()).equalTo(0)
      .with(new JoinFunction<Subgraph<Long, GD>, Tuple1<Long>, GD>() {
        @Override
        public GD join(Subgraph<Long, GD> g, Tuple1<Long> gID) throws
          Exception {
          return g.getValue();
        }
      }).first(1).collect();

    return (graphData.size() > 0) ? LogicalGraph
      .fromGellyGraph(subGraph, graphData.get(0), getVertexDataFactory(),
        getEdgeDataFactory(), getGraphDataFactory()) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> getGraphs(final Long... identifiers) throws
    Exception {
    return getGraphs(Arrays.asList(identifiers));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> getGraphs(
    final List<Long> identifiers) throws Exception {

    DataSet<Subgraph<Long, GD>> newSubGraphs =
      this.subgraphs.filter(new FilterFunction<Subgraph<Long, GD>>() {

        @Override
        public boolean filter(Subgraph<Long, GD> subgraph) throws Exception {
          return identifiers.contains(subgraph.getId());

        }
      });

    // build new vertex set
    DataSet<Vertex<Long, VD>> vertices = getVertices()
      .filter(new VertexInGraphsFilter<VD>(identifiers));

    // build new edge set
    DataSet<Edge<Long, ED>> edges = getEdges()
      .filter(new EdgeInGraphsFilter<ED>(identifiers));

    return new GraphCollection<>(Graph.fromDataSet(vertices, edges,
      getExecutionEnvironment()),
      newSubGraphs,
      getVertexDataFactory(),
      getEdgeDataFactory(),
      getGraphDataFactory(),
      getExecutionEnvironment());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getGraphCount() throws Exception {
    return this.subgraphs.count();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> filter(
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
    Graph<Long, VD, ED> filteredGraph = getGellyGraph().filterOnVertices(

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

    return new GraphCollection<>(filteredGraph,
      filteredSubgraphs,
      getVertexDataFactory(),
      getEdgeDataFactory(),
      getGraphDataFactory(),
      getExecutionEnvironment());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> select(
    Predicate<LogicalGraph<VD, ED, GD>> predicateFunction) throws Exception {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> union(
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Union<VD, ED, GD>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> intersect(
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Intersect<VD, ED, GD>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> intersectWithSmall(
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new IntersectUsingList<VD, ED, GD>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> difference(
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new Difference<VD, ED, GD>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> differenceWithSmallResult(
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return callForCollection(new DifferenceUsingList<VD, ED, GD>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> distinct() {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> top(int limit) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> apply(
    UnaryGraphToGraphOperator<VD, ED, GD> op) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> reduce(
    BinaryGraphToGraphOperator<VD, ED, GD> op) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> callForCollection(
    UnaryCollectionToCollectionOperator<VD, ED, GD> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> callForCollection(
    BinaryCollectionToCollectionOperator<VD, ED, GD> op,
    GraphCollection<VD, ED, GD> otherCollection) throws Exception {
    return op.execute(this, otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> callForGraph(
    UnaryCollectionToGraphOperator<VD, ED, GD> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeAsJson(String vertexFile, String edgeFile,
    String graphFile) throws Exception {
    this.getGellyGraph().getVertices().writeAsFormattedText(vertexFile,
      new JsonWriter.VertexTextFormatter<VD>());
    this.getGellyGraph().getEdges()
      .writeAsFormattedText(edgeFile, new JsonWriter.EdgeTextFormatter<ED>());
    this.getSubgraphs()
      .writeAsFormattedText(graphFile, new JsonWriter.GraphTextFormatter<GD>());
    getExecutionEnvironment().execute();
  }

  /**
   * Checks if a vertex is contained in the given graph.
   *
   * @param <VD> vertex data type
   */
  private static class VertexInGraphFilter<VD extends VertexData> implements
    FilterFunction<Vertex<Long, VD>> {

    /**
     * Graph identifier
     */
    private final long graphId;

    /**
     * Creates a filter
     *
     * @param graphId graphId for containment check
     */
    public VertexInGraphFilter(long graphId) {
      this.graphId = graphId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(Vertex<Long, VD> v) throws Exception {
      return (v.getValue().getGraphCount() > 0) &&
        v.getValue().getGraphs().contains(graphId);
    }
  }

  /**
   * Checks if an edge is contained in the given graph.
   *
   * @param <ED> edge data type
   */
  private static class EdgeInGraphFilter<ED extends EdgeData> implements
    FilterFunction<Edge<Long, ED>> {

    /**
     * Graph identifier
     */
    private final long graphId;

    /**
     * Creates a filter
     *
     * @param graphId graphId for containment check
     */
    public EdgeInGraphFilter(long graphId) {
      this.graphId = graphId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean filter(Edge<Long, ED> e) throws Exception {
      return (e.getValue().getGraphCount() > 0) &&
        e.getValue().getGraphs().contains(graphId);
    }
  }

  /**
   * Checks if a vertex is contained in at least one of the given logical
   * graphs.
   *
   * @param <VD> vertex data type
   */
  private static class VertexInGraphsFilter<VD extends VertexData> implements
    FilterFunction<Vertex<Long, VD>> {

    /**
     * Graph identifiers
     */
    private final List<Long> identifiers;

    /**
     * Creates a filter
     *
     * @param identifiers graph identifiers for containment check
     */
    public VertexInGraphsFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    /**
     * {@inheritDoc}
     */
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

  /**
   * Checks if an edge is contained in at least one of the given logical
   * graphs.
   *
   * @param <ED> edge data type
   */
  private static class EdgeInGraphsFilter<ED extends EdgeData> implements
    FilterFunction<Edge<Long, ED>> {

    /**
     * Graph identifiers
     */
    private final List<Long> identifiers;

    /**
     * Creates a filter
     *
     * @param identifiers graph identifiers for containment check
     */
    public EdgeInGraphsFilter(List<Long> identifiers) {
      this.identifiers = identifiers;
    }

    /**
     * {@inheritDoc}
     */
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
