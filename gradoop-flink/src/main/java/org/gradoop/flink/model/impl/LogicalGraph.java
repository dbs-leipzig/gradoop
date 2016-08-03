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

package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.api.operators.LogicalGraphOperators;
import org.gradoop.flink.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.PropertyGetter;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.equality.GraphEquality;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.grouping.Grouping.GroupingBuilder;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.sampling.RandomNodeSampling;
import org.gradoop.flink.model.impl.operators.split.Split;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToIdString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToIdString;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a logical graph inside the EPGM.
 */
public class LogicalGraph extends GraphBase implements LogicalGraphOperators {

  /**
   * Creates a new logical graph based on the given parameters.
   *
   * @param graphHead graph head data set associated with that graph
   * @param vertices  vertex data set
   * @param edges     edge data set
   * @param config    Gradoop Flink configuration
   */
  private LogicalGraph(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config) {
    super(graphHead, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Factory methods
  //----------------------------------------------------------------------------

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param vertices  Vertex dataset
   * @param config    Gradoop Flink configuration
   * @return Logical graph
   */
  public static LogicalGraph fromDataSets(DataSet<Vertex> vertices,
    GradoopFlinkConfig config) {
    return fromDataSets(vertices,
      createEdgeDataSet(Lists.<Edge>newArrayListWithCapacity(0), config),
      config);
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
   * @return Logical graph
   */
  public static LogicalGraph fromDataSets(DataSet<GraphHead> graphHead,
    DataSet<Vertex> vertices, DataSet<Edge> edges, GradoopFlinkConfig config) {
    return new LogicalGraph(graphHead, vertices, edges, config);
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
   * @return Logical graph
   */
  public static LogicalGraph fromDataSets(DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config) {

    checkNotNull(vertices, "Vertex DataSet was null");
    checkNotNull(edges, "Edge DataSet was null");
    checkNotNull(config, "Config was null");
    GraphHead graphHead = config
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<GraphHead> graphHeadSet = config.getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices.map(new AddToGraph<Vertex>(graphHead));
    edges = edges.map(new AddToGraph<Edge>(graphHead));

    return new LogicalGraph(graphHeadSet, vertices, edges, config);
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param graphHead   Graph head associated with the logical graph
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  @SuppressWarnings("unchecked")
  public static LogicalGraph fromCollections(GraphHead graphHead,
    Collection<Vertex> vertices, Collection<Edge> edges,
    GradoopFlinkConfig config) {

    List<GraphHead> graphHeads;
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
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @param config      Gradoop Flink configuration
   * @return Logical graph
   */
  public static LogicalGraph fromCollections(Collection<Vertex> vertices,
    Collection<Edge> edges, GradoopFlinkConfig config) {

    checkNotNull(vertices, "Vertex collection was null");
    checkNotNull(edges, "Edge collection was null");
    checkNotNull(config, "Config was null");

    GraphHead graphHead = config.getGraphHeadFactory().createGraphHead();

    DataSet<Vertex> vertexDataSet = createVertexDataSet(vertices, config)
      .map(new AddToGraph<Vertex>(graphHead));

    DataSet<Edge> edgeDataSet = createEdgeDataSet(edges, config)
      .map(new AddToGraph<Edge>(graphHead));

    return fromDataSets(
      createGraphHeadDataSet(new ArrayList<GraphHead>(0), config),
      vertexDataSet, edgeDataSet, config
    );
  }

  /**
   * Creates an empty graph collection.
   *
   * @param config  Gradoop Flink configuration
   * @return empty graph collection
   */
  public static LogicalGraph createEmptyGraph(GradoopFlinkConfig config) {
    checkNotNull(config, "Config was null");

    Collection<Vertex> vertices = new ArrayList<>(0);
    Collection<Edge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges, config);
  }

  //----------------------------------------------------------------------------
  // Containment methods
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  public DataSet<GraphHead> getGraphHead() {
    return super.getGraphHeads();
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection match(String pattern) {
    return match(pattern, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection match(String pattern, boolean attachData) {
    return callForCollection(new ExplorativeSubgraphIsomorphism(
      pattern, attachData, new DFSTraverser()));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph copy() {
    return callForGraph(
      new Cloning());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph transform(
    TransformationFunction<GraphHead> graphHeadTransformationFunction,
    TransformationFunction<Vertex> vertexTransformationFunction,
    TransformationFunction<Edge> edgeTransformationFunction) {
    return callForGraph(new Transformation(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  @Override
  public LogicalGraph transformGraphHead(
    TransformationFunction<GraphHead> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  @Override
  public LogicalGraph transformVertices(
    TransformationFunction<Vertex> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  @Override
  public LogicalGraph transformEdges(
    TransformationFunction<Edge> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph vertexInducedSubgraph(
    FilterFunction<Vertex> vertexFilterFunction) {
    checkNotNull(vertexFilterFunction);
    return callForGraph(new Subgraph(vertexFilterFunction, null));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph edgeInducedSubgraph(
    FilterFunction<Edge> edgeFilterFunction) {
    checkNotNull(edgeFilterFunction);
    return callForGraph(new Subgraph(null, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction) {
    checkNotNull(vertexFilterFunction);
    checkNotNull(edgeFilterFunction);
    return callForGraph(
      new Subgraph(vertexFilterFunction, edgeFilterFunction));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph aggregate(String propertyKey,
    AggregateFunction aggregateFunc) {
    return callForGraph(new Aggregation(propertyKey, aggregateFunc));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph sampleRandomNodes(Float sampleSize) {
    return callForGraph(new RandomNodeSampling(sampleSize));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupBy(List<String> vertexGroupingKeys,
    List<String> edgeGroupingKeys) {
    GroupingBuilder builder = new GroupingBuilder();

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
  public LogicalGraph groupByVertexLabel() {
    return groupByVertexLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexLabelAndVertexProperties(
    List<String> vertexGroupingKeys) {
    return groupByVertexLabel(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexLabelAndEdgeProperties(
    List<String> edgeGroupingKeys) {
    return groupByVertexLabel(null, edgeGroupingKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexLabel(List<String> vertexGroupingKeys,
    List<String> edgeGroupingKeys) {
    GroupingBuilder builder = new GroupingBuilder();

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
  public LogicalGraph groupByVertexAndEdgeLabel() {
    return groupByVertexAndEdgeLabel(null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexAndEdgeLabelAndVertexProperties(
    List<String> vertexGroupingKeys) {
    return groupByVertexAndEdgeLabel(vertexGroupingKeys, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexAndEdgeLabelAndEdgeProperties(
    List<String> edgeGroupingKeys) {
    return groupByVertexAndEdgeLabel(null, edgeGroupingKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph groupByVertexAndEdgeLabel(
    List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    GroupingBuilder builder = new GroupingBuilder();

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
  public LogicalGraph combine(LogicalGraph otherGraph) {
    return callForGraph(new Combination(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph overlap(LogicalGraph otherGraph) {
    return callForGraph(new Overlap(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph exclude(LogicalGraph otherGraph) {
    return callForGraph(new Exclusion(), otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementIds(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToIdString(),
      new EdgeToIdString(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByElementData(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToEmptyString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Boolean> equalsByData(LogicalGraph other) {
    return new GraphEquality(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(this, other);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph callForGraph(UnaryGraphToGraphOperator operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph callForGraph(BinaryGraphToGraphOperator operator,
    LogicalGraph otherGraph) {
    return operator.execute(this, otherGraph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection callForCollection(
    UnaryGraphToCollectionOperator operator) {
    return operator.execute(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection splitBy(String propertyKey) {
    return callForCollection(
      new Split(
        new PropertyGetter<Vertex>(Lists.newArrayList(propertyKey))));
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
      .map(new True<Vertex>())
      .distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false))
      .reduce(new Or())
      .map(new Not());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(this);
  }
}
