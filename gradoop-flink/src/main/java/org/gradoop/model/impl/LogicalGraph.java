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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.PredicateFunction;
import org.gradoop.model.api.functions.ProjectionFunction;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.api.operators.LogicalGraphOperators;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.bool.True;
import org.gradoop.model.impl.functions.graphcontainment.GraphContainmentUpdater;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.Aggregation;
import org.gradoop.model.impl.operators.combination.Combination;
import org.gradoop.model.impl.operators.equality.EqualityByElementData;
import org.gradoop.model.impl.operators.equality.EqualityByElementIds;
import org.gradoop.model.impl.operators.exclusion.Exclusion;
import org.gradoop.model.impl.operators.overlap.Overlap;
import org.gradoop.model.impl.operators.projection.Projection;
import org.gradoop.model.impl.operators.sampling.RandomNodeSampling;
import org.gradoop.model.impl.operators.summarization.SummarizationGroupCombine;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a logical graph inside the EPGM.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class LogicalGraph
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GraphBase<G, V, E>
  implements LogicalGraphOperators<G, V, E> {

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param graphHead graph head data set associated with that graph
   * @param vertices  vertex data set
   * @param edges     edge data set
   * @param config    Gradoop Flink configuration
   */
  private LogicalGraph(
    DataSet<G> graphHead, DataSet<V> vertices, DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    super(graphHead, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Factory methods
  //----------------------------------------------------------------------------

  /**
   * Creates a logical graph from the given Gelly graph. A new graph head will
   * created, all vertices and edges will be assigned to that logical graph.
   *
   * @param graph     Flink Gelly graph
   * @param config    Gradoop Flink configuration
   * @param <G>       EPGM graph head type
   * @param <V>       EPGM vertex type
   * @param <E>       EPGM edge type
   * @return Logical Graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> LogicalGraph<G, V, E> fromGellyGraph(
    Graph<GradoopId, V, E> graph,
    GradoopFlinkConfig<G, V, E> config) {

    return fromDataSets(
      graph.getVertices().map(new MapFunction<Vertex<GradoopId, V>, V>() {
        @Override
        public V map(Vertex<GradoopId, V> gellyVertex) throws Exception {
          return gellyVertex.getValue();
        }
      }).withForwardedFields("f1->*"),
      graph.getEdges().map(new MapFunction<Edge<GradoopId, E>, E>() {
        @Override
        public E map(Edge<GradoopId, E> gellyEdge) throws Exception {
          return gellyEdge.getValue();
        }
      }).withForwardedFields("f2->*"), config);
  }

  /**
   * Creates a logical graph from the given Gelly graph. All vertices and edges
   * will be assigned to the given graph head.
   *
   * @param graph     Flink Gelly graph
   * @param graphHead EPGM graph head for the logical graph
   * @param config    Gradoop Flink configuration
   * @param <G>       EPGM graph head type
   * @param <V>       EPGM vertex type
   * @param <E>       EPGM edge type
   * @return Logical Graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> LogicalGraph<G, V, E> fromGellyGraph(
    Graph<GradoopId, V, E> graph,
    G graphHead,
    GradoopFlinkConfig<G, V, E> config) {
    return fromDataSets(
      config.getExecutionEnvironment().fromElements(graphHead),
      graph.getVertices().map(new MapFunction<Vertex<GradoopId, V>, V>() {
        @Override
        public V map(Vertex<GradoopId, V> gellyVertex) throws Exception {
          return gellyVertex.getValue();
        }
      }).withForwardedFields("f1->*"),
      graph.getEdges().map(new MapFunction<Edge<GradoopId, E>, E>() {
        @Override
        public E map(Edge<GradoopId, E> gellyEdge) throws Exception {
          return gellyEdge.getValue();
        }
      }).withForwardedFields("f2->*"), config);
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param <G>         EPGM graph head graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> LogicalGraph<G, V, E> fromDataSets(
    DataSet<G> graphHead, DataSet<V> vertices, DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {
    return new LogicalGraph<>(graphHead, vertices, edges, config);
  }

  /**
   * Creates a logical graph from the given argument.
   *
   * The method creates a new graph head element and assigns the vertices and
   * edges to that graph.
   *
   * @param <G>         EPGM graph head graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> LogicalGraph<G, V, E> fromDataSets(
    DataSet<V> vertices, DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    checkNotNull(vertices, "Vertex DataSet was null");
    checkNotNull(edges, "Edge DataSet was null");
    checkNotNull(config, "Config was null");
    G graphHead = config
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<G> graphHeadSet = config.getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices.map(new GraphContainmentUpdater<G, V>(graphHead));
    edges = edges.map(new GraphContainmentUpdater<G, E>(graphHead));

    return new LogicalGraph<>(
      graphHeadSet,
      vertices,
      edges,
      config
    );
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param <G>         EPGM graph type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param graphHead   Graph head associated with the logical graph
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  @SuppressWarnings("unchecked")
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  LogicalGraph<G, V, E> fromCollections(
    G graphHead,
    Collection<V> vertices,
    Collection<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    List<G> graphHeads;
    if (graphHead == null) {
      graphHeads = Lists.newArrayListWithCapacity(0);
    } else {
      graphHeads = Lists.newArrayList(graphHead);
    }

    checkNotNull(vertices, "Vertex collection was null");
    checkNotNull(edges, "Edge collection was null");
    checkNotNull(config, "Config was null");
    return fromDataSets(
      createGraphHeadDataSet(graphHeads, config),
      createVertexDataSet(vertices, config),
      createEdgeDataSet(edges, config),
      config
    );
  }

  /**
   * Creates a logical graph from the given arguments. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param <G>         EPGM graph type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  public static
  <V extends EPGMVertex, E extends EPGMEdge, G extends EPGMGraphHead>
  LogicalGraph<G, V, E> fromCollections(
    Collection<V> vertices,
    Collection<E> edges,
    GradoopFlinkConfig<G, V, E> config) {

    checkNotNull(vertices, "Vertex collection was null");
    checkNotNull(edges, "Edge collection was null");
    checkNotNull(config, "Config was null");
    return fromDataSets(
      createGraphHeadDataSet(new ArrayList<G>(0), config),
      createVertexDataSet(vertices, config),
      createEdgeDataSet(edges, config),
      config
    );
  }

  /**
   * Creates an empty graph collection.
   *
   * @param config  Gradoop Flink configuration
   * @param <G>     EPGM graph head type
   * @param <V>     EPGM vertex type
   * @param <E>     EPGM edge type
   * @return empty graph collection
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  LogicalGraph<G, V, E> createEmptyGraph(GradoopFlinkConfig<G, V, E> config) {
    checkNotNull(config, "Config was null");

    Collection<V> vertices = new ArrayList<>(0);
    Collection<E> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Containment methods
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  public DataSet<G> getGraphHead() {
    return this.graphHeads;
  }

  //----------------------------------------------------------------------------
  // Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> match(String graphPattern,
    PredicateFunction<LogicalGraph> predicateFunc) {
    throw new NotImplementedException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> project(ProjectionFunction<V> vertexFunction,
    ProjectionFunction<E> edgeFunction) throws Exception {
    return callForGraph(
      new Projection<G, V, E>(vertexFunction, edgeFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <N extends Number> LogicalGraph<G, V, E> aggregate(String propertyKey,
    AggregateFunction<N, G, V, E> aggregateFunc) throws Exception {

    return callForGraph(new Aggregation<>(propertyKey, aggregateFunc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> sampleRandomNodes(Float
    sampleSize) throws Exception {
    return callForGraph(new RandomNodeSampling<G, V, E>(sampleSize));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarize(String vertexGroupingKey) throws
    Exception {
    return summarize(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarize(String vertexGroupingKey,
    String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationGroupCombine<G, V, E>(vertexGroupingKey,
        edgeGroupingKey, false, false));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexLabel() throws Exception {
    return summarizeOnVertexLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception {
    return summarizeOnVertexLabel(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception {
    return summarizeOnVertexLabel(null, edgeGroupingKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexLabel(
    String vertexGroupingKey, String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationGroupCombine<G, V, E>(vertexGroupingKey,
        edgeGroupingKey, true, false));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabel() throws
    Exception {
    return summarizeOnVertexAndEdgeLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E>
  summarizeOnVertexAndEdgeLabelAndVertexProperty(
    String vertexGroupingKey) throws Exception {
    return summarizeOnVertexAndEdgeLabel(vertexGroupingKey, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabelAndEdgeProperty(
    String edgeGroupingKey) throws Exception {
    return summarizeOnVertexAndEdgeLabel(null, edgeGroupingKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> summarizeOnVertexAndEdgeLabel(
    String vertexGroupingKey, String edgeGroupingKey) throws Exception {
    return callForGraph(
      new SummarizationGroupCombine<G, V, E>(vertexGroupingKey,
        edgeGroupingKey, true, true));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> combine(LogicalGraph<G, V, E> otherGraph) {
    return callForGraph(new Combination<G, V, E>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> overlap(LogicalGraph<G, V, E> otherGraph) {
    return callForGraph(new Overlap<G, V, E>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> exclude(LogicalGraph<G, V, E> otherGraph) {
    return callForGraph(new Exclusion<G, V, E>(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> callForGraph(
    UnaryGraphToGraphOperator<G, V, E> operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> callForGraph(
    BinaryGraphToGraphOperator<G, V, E> operator,
    LogicalGraph<G, V, E> otherGraph) {
    return operator.execute(this, otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> callForCollection(
    UnaryGraphToCollectionOperator<G, V, E> operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementIds(LogicalGraph<G, V, E> other) {
    return new EqualityByElementIds<G, V, E>().execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementData(LogicalGraph<G, V, E> other) {
    return new EqualityByElementData<G, V, E>().execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices()
      .map(new True<V>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }
}
