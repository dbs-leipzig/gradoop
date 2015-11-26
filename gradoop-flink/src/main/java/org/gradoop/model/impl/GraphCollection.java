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
import org.gradoop.io.json.JsonWriter;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.api.operators.GraphCollectionOperators;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.functions.Predicate;
import org.gradoop.model.impl.functions.filterfunctions.False;
import org.gradoop.model.impl.functions.filterfunctions.EdgesByGraphId;
import org.gradoop.model.impl.functions.filterfunctions.EdgeInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .EdgeInGraphsFilterWithBC;
import org.gradoop.model.impl.functions.filterfunctions.GraphHeadByGraphID;
import org.gradoop.model.impl.functions.filterfunctions.VerticesByGraphId;
import org.gradoop.model.impl.functions.filterfunctions.VertexInGraphsFilter;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInGraphsFilterWithBC;
import org.gradoop.model.impl.functions.keyselectors.GraphKeySelector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.collection.binary.Difference;
import org.gradoop.model.impl.operators.collection.binary.DifferenceUsingList;
import org.gradoop.model.impl.operators.collection.binary.Intersect;
import org.gradoop.model.impl.operators.collection.binary.IntersectUsingList;
import org.gradoop.model.impl.operators.collection.binary.Union;
import org.gradoop.model.impl.operators.equality.collection
  .EqualByGraphElementIds;
import org.gradoop.model.impl.operators.equality.collection.EqualByGraphIds;
import org.gradoop.util.GradoopFlinkConfig;
import org.gradoop.util.Order;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a collection of graphs inside the EPGM. As graphs may share
 * vertices and edges, the collections contains a single gelly graph
 * representing all subgraphs. Graph data is stored in an additional dataset.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class GraphCollection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends AbstractGraph<G, V, E>
  implements GraphCollectionOperators<G, V, E> {

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param vertices    vertices
   * @param edges       edges
   * @param graphHeads  graph heads
   * @param config      Gradoop Flink configuration
   */
  private GraphCollection(DataSet<V> vertices,
    DataSet<E> edges,
    DataSet<G> graphHeads,
    GradoopFlinkConfig<V, E, G> config) {
    super(graphHeads, vertices, edges, config);
  }

  /**
   * Creates a graph collection from the given arguments.
   *
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param graphHeads  GraphHead DataSet
   * @param config      Gradoop Flink configuration
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param <G>         EPGM graph head graph head type
   * @return Graph collection
   */
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  GraphCollection<G, V, E>
  fromDataSets(DataSet<V> vertices,
    DataSet<E> edges,
    DataSet<G> graphHeads,
    GradoopFlinkConfig<V, E, G> config) {
    return new GraphCollection<>(vertices, edges, graphHeads, config);
  }

  /**
   *
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param graphHeads  Graph Head collection
   * @param config      Gradoop Flink configuration
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param <G>         EPGM graph type
   * @return Graph collection
   */
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  GraphCollection<G, V, E>
  fromCollections(
    Collection<V> vertices,
    Collection<E> edges,
    Collection<G> graphHeads,
    GradoopFlinkConfig<V, E, G> config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<G> graphHeadSet;
    if(vertices.isEmpty()) {
      graphHeads.add(config.getGraphHeadFactory().createGraphHead());
      graphHeadSet = env.fromCollection(graphHeads)
        .filter(new False<G>());
    } else {
      graphHeadSet =  env.fromCollection(graphHeads);
    }

    DataSet<V> vertexSet;
    if(vertices.isEmpty()) {
      vertices.add(config.getVertexFactory().createVertex());
      vertexSet = env.fromCollection(vertices)
        .filter(new False<V>());
    } else {
      vertexSet = env.fromCollection(vertices);
    }

    GradoopId dummyId = GradoopId.get();
    DataSet<E> edgeSet;
    if(vertices.isEmpty()) {
      edges.add(config.getEdgeFactory().createEdge(dummyId, dummyId));
      edgeSet = env.fromCollection(edges)
        .filter(new False<E>());
    } else {
      edgeSet = env.fromCollection(edges);
    }

    return fromDataSets(vertexSet, edgeSet, graphHeadSet, config);
  }

  /**
   * Returns the graph heads associated with the logical graphs in that
   * collection.
   *
   * @return graph heads
   */
  public DataSet<G> getGraphHeads() {
    return this.graphHeads;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public LogicalGraph<G, V, E> getGraph(final GradoopId graphID) throws
    Exception {
    // filter vertices and edges based on given graph id
    DataSet<G> graphHead = getGraphHeads()
      .filter(new GraphHeadByGraphID<G>(graphID));

    DataSet<V> vertices = getVertices()
      .filter(new VerticesByGraphId<V>(graphID));
    DataSet<E> edges = getEdges()
      .filter(new EdgesByGraphId<E>(graphID));

    DataSet<Tuple1<GradoopId>> graphIDDataSet = getConfig()
      .getExecutionEnvironment()
      .fromCollection(Lists.newArrayList(new Tuple1<>(graphID)));

    // get graph data based on graph id
    List<G> graphData = this.graphHeads
      .joinWithTiny(graphIDDataSet)
      .where(new GraphKeySelector<G>())
      .equalTo(0)
      .with(new JoinFunction<G, Tuple1<GradoopId>, G>() {
        @Override
        public G join(G g, Tuple1<GradoopId> gID) throws Exception {
          return g;
        }
      }).first(1).collect();

    return (graphData.size() > 0) ? LogicalGraph
      .fromDataSets(graphHead, vertices, edges, getConfig()) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E>
  getGraphs(final GradoopId... identifiers) throws Exception {

    GradoopIdSet graphIds = new GradoopIdSet();

    for (GradoopId id : identifiers) {
      graphIds.add(id);
    }

    return getGraphs(graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> getGraphs(
    final GradoopIdSet identifiers) throws Exception {

    DataSet<G> newGraphHeads =
      this.graphHeads.filter(new FilterFunction<G>() {

        @Override
        public boolean filter(G graphHead) throws Exception {
          return identifiers.contains(graphHead.getId());

        }
      });

    // build new vertex set
    DataSet<V> vertices = getVertices()
      .filter(new VertexInGraphsFilter<V>(identifiers));

    // build new edge set
    DataSet<E> edges = getEdges()
      .filter(new EdgeInGraphsFilter<E>(identifiers));

    return new GraphCollection<>(vertices, edges, newGraphHeads, getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getGraphCount() throws Exception {
    return this.graphHeads.count();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> filter(
    final Predicate<G> predicateFunction) throws Exception {
    // find graph heads matching the predicate
    DataSet<G> filteredGraphHeads =
      this.graphHeads.filter(new FilterFunction<G>() {
        @Override
        public boolean filter(G g) throws Exception {
          return predicateFunction.filter(g);
        }
      });

    // get the identifiers of these subgraphs
    DataSet<GradoopId> graphIDs =
      filteredGraphHeads.map(new MapFunction<G, GradoopId>() {
        @Override
        public GradoopId map(G g) throws Exception {
          return g.getId();
        }
      });

    // use graph ids to filter vertices from the actual graph structure
    DataSet<V> vertices = getVertices()
      .filter(new VertexInGraphsFilterWithBC<V>())
      .withBroadcastSet(graphIDs, VertexInGraphsFilterWithBC.BC_IDENTIFIERS);
    DataSet<E> edges = getEdges()
      .filter(new EdgeInGraphsFilterWithBC<E>())
      .withBroadcastSet(graphIDs, EdgeInGraphsFilterWithBC.BC_IDENTIFIERS);

    return new GraphCollection<>(vertices, edges, filteredGraphHeads,
      getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> select(
    Predicate<LogicalGraph<G, V, E>> predicateFunction) throws Exception {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> union(
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return callForCollection(new Union<V, E, G>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> intersect(
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return callForCollection(new Intersect<V, E, G>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> intersectWithSmallResult(
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return callForCollection(new IntersectUsingList<V, E, G>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> difference(
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return callForCollection(new Difference<V, E, G>(), otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> differenceWithSmallResult(
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return callForCollection(new DifferenceUsingList<V, E, G>(),
      otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> distinct() {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> sortBy(String propertyKey, Order order) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> top(int limit) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> apply(
    UnaryGraphToGraphOperator<V, E, G> op) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> reduce(
    BinaryGraphToGraphOperator<G, V, E> op) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> callForCollection(
    UnaryCollectionToCollectionOperator<V, E, G> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> callForCollection(
    BinaryCollectionToCollectionOperator<V, E, G> op,
    GraphCollection<G, V, E> otherCollection) throws Exception {
    return op.execute(this, otherCollection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> callForGraph(
    UnaryCollectionToGraphOperator<G, V, E> op) {
    return op.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeAsJson(String vertexFile, String edgeFile,
    String graphFile) throws Exception {
    getVertices().writeAsFormattedText(vertexFile,
      new JsonWriter.VertexTextFormatter<V>());
    getEdges().writeAsFormattedText(edgeFile,
      new JsonWriter.EdgeTextFormatter<E>());
    getGraphHeads().writeAsFormattedText(graphFile,
      new JsonWriter.GraphTextFormatter<G>());
    getConfig().getExecutionEnvironment().execute();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphIds(GraphCollection<G, V, E> other) {
    return new EqualByGraphIds<G, V, E>().execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean equalsByGraphIdsCollected(
    GraphCollection<G, V, E> other) throws Exception {
    return collectEquals(equalsByGraphIds(other));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByGraphElementIds(
    GraphCollection<G, V, E> other) {
    return new EqualByGraphElementIds<G, V, E>().execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean equalsByGraphElementIdsCollected(
    GraphCollection<G, V, E> other) throws Exception {
    return collectEquals(equalsByGraphElementIds(other));
  }

  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  GraphCollection<G, V, E> 
  createEmptyCollection(
    GradoopFlinkConfig<V, E, G> config) {
    Collection<G> graphHeads = new ArrayList<>();
    Collection<V> vertices = new ArrayList<>();
    Collection<E> edges = new ArrayList<>();
    
    return GraphCollection.fromCollections(
      vertices, edges, graphHeads, config);
  }
}
