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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.api.DataSink;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.AggregateFunction;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.model.api.operators.LogicalGraphOperators;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.bool.True;
import org.gradoop.model.impl.functions.epgm.PropertyGetter;
import org.gradoop.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.Aggregation;
import org.gradoop.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.model.impl.operators.combination.Combination;
import org.gradoop.model.impl.operators.equality.GraphEquality;
import org.gradoop.model.impl.operators.exclusion.Exclusion;
import org.gradoop.model.impl.operators.transformation.Transformation;
import org.gradoop.model.impl.operators.overlap.Overlap;
import org.gradoop.model.impl.operators.cloning.Cloning;
import org.gradoop.model.impl.operators.sampling.RandomNodeSampling;
import org.gradoop.model.impl.operators.split.Split;
import org.gradoop.model.impl.operators.subgraph.Subgraph;
import org.gradoop.model.impl.operators.grouping.Grouping.GroupingBuilder;
import org.gradoop.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;
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
   * @param vertices  Vertex dataset
   * @param config    Gradoop Flink configuration
   * @param <G>       EPGM graph head graph head type
   * @param <V>       EPGM vertex type
   * @param <E>       EPGM edge type
   * @return Logical graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> LogicalGraph<G, V, E> fromDataSets(
    DataSet<V> vertices, GradoopFlinkConfig<G, V, E> config) {
    return fromDataSets(vertices,
      createEdgeDataSet(Lists.<E>newArrayListWithCapacity(0), config), config);
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param config      Gradoop Flink configuration
   * @param <G>         EPGM graph head graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
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
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @param config      Gradoop Flink configuration
   * @param <G>         EPGM graph head graph head type
   * @param <V>         EPGM vertex type
   * @param <E>         EPGM edge type
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
    vertices = vertices.map(new AddToGraph<G, V>(graphHead));
    edges = edges.map(new AddToGraph<G, E>(graphHead));

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

    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
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

    G graphHead = config.getGraphHeadFactory().createGraphHead();

    DataSet<V> vertexDataSet = createVertexDataSet(vertices, config)
      .map(new AddToGraph<G, V>(graphHead));

    DataSet<E> edgeDataSet = createEdgeDataSet(edges, config)
      .map(new AddToGraph<G, E>(graphHead));

    return fromDataSets(
      createGraphHeadDataSet(new ArrayList<G>(0), config),
      vertexDataSet,
      edgeDataSet,
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
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> match(String pattern) {
    return match(pattern, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<G, V, E> match(String pattern, boolean attachData) {
    return callForCollection(new ExplorativeSubgraphIsomorphism<G, V, E>(
      pattern, attachData, new DFSTraverser()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> copy() {
    return callForGraph(
      new Cloning<G, V, E>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  @Override
  public LogicalGraph<G, V, E> transformGraphHead(
    TransformationFunction<G> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  @Override
  public LogicalGraph<G, V, E> transformVertices(
    TransformationFunction<V> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  @Override
  public LogicalGraph<G, V, E> transformEdges(
    TransformationFunction<E> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> vertexInducedSubgraph(
    FilterFunction<V> vertexFilterFunction) {
    checkNotNull(vertexFilterFunction);
    return callForGraph(new Subgraph<G, V, E>(vertexFilterFunction, null));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> edgeInducedSubgraph(
    FilterFunction<E> edgeFilterFunction) {
    checkNotNull(edgeFilterFunction);
    return callForGraph(new Subgraph<G, V, E>(null, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> subgraph(FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction) {
    checkNotNull(vertexFilterFunction);
    checkNotNull(edgeFilterFunction);
    return callForGraph(
      new Subgraph<G, V, E>(vertexFilterFunction, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> aggregate(String propertyKey,
    AggregateFunction<G, V, E> aggregateFunc) {
    return callForGraph(new Aggregation<>(propertyKey, aggregateFunc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> sampleRandomNodes(Float sampleSize) {
    return callForGraph(new RandomNodeSampling<G, V, E>(sampleSize));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupBy(List<String> vertexGroupingKeys,
    List<String> edgeGroupingKeys) {
    GroupingBuilder<G, V, E> builder = new GroupingBuilder<>();

    if (vertexGroupingKeys != null) {
      builder.addVertexGroupingKeys(vertexGroupingKeys);
    }
    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }

    return callForGraph(builder
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .useVertexLabel(false)
        .useEdgeLabel(false)
        .addVertexAggregator(new CountAggregator())
        .addEdgeAggregator(new CountAggregator())
        .build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexLabel() {
    return groupByVertexLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexLabelAndVertexProperties(
    List<String> vertexGroupingKeys) {
    return groupByVertexLabel(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexLabelAndEdgeProperties(
    List<String> edgeGroupingKeys) {
    return groupByVertexLabel(null, edgeGroupingKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexLabel(
    List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    GroupingBuilder<G, V, E> builder = new GroupingBuilder<>();

    if (vertexGroupingKeys != null) {
      builder.addVertexGroupingKeys(vertexGroupingKeys);
    }
    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }
    return callForGraph(builder
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .useVertexLabel(true)
        .useEdgeLabel(false)
        .addVertexAggregator(new CountAggregator())
        .addEdgeAggregator(new CountAggregator())
        .build());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexAndEdgeLabel() {
    return groupByVertexAndEdgeLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexAndEdgeLabelAndVertexProperties(
    List<String> vertexGroupingKeys) {
    return groupByVertexAndEdgeLabel(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexAndEdgeLabelAndEdgeProperties(
    List<String> edgeGroupingKeys) {
    return groupByVertexAndEdgeLabel(null, edgeGroupingKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> groupByVertexAndEdgeLabel(
    List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    GroupingBuilder<G, V, E> builder = new GroupingBuilder<>();

    if (vertexGroupingKeys != null) {
      builder.addVertexGroupingKeys(vertexGroupingKeys);
    }
    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }
    return callForGraph(builder
        .setStrategy(GroupingStrategy.GROUP_REDUCE)
        .useVertexLabel(true)
        .useEdgeLabel(true)
        .addVertexAggregator(new CountAggregator())
        .addEdgeAggregator(new CountAggregator())
        .build());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

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
  public DataSet<Boolean> equalsByElementIds(LogicalGraph<G, V, E> other) {
    return new GraphEquality<>(
      new GraphHeadToEmptyString<G>(),
      new VertexToIdString<V>(),
      new EdgeToIdString<E>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementData(LogicalGraph<G, V, E> other) {
    return new GraphEquality<>(
      new GraphHeadToEmptyString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByData(LogicalGraph<G, V, E> other) {
    return new GraphEquality<>(
      new GraphHeadToDataString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

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
  public GraphCollection<G, V, E> splitBy(
    String propertyKey, boolean preserveInterEdges) {
    return callForCollection(
      new Split<G, V, E>(
        new PropertyGetter<V>(Lists.newArrayList(propertyKey)),
        preserveInterEdges));
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(DataSink<G, V, E> dataSink) throws IOException {
    dataSink.write(this);
  }
}
