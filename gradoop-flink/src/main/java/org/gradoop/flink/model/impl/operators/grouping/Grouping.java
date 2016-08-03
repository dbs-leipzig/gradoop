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

package org.gradoop.flink.model.impl.operators.grouping;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions
  .BuildEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions
  .CombineEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.UpdateEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;


import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The grouping operator determines a structural grouping of vertices and edges
 * to condense a graph and thus help to uncover insights about patterns and
 * statistics hidden in the graph.
 * <p>
 * The graph grouping operator represents every vertex group by a single super
 * vertex in the resulting graph; (super) edges between vertices in the
 * resulting graph represent a group of edges between the vertex group members
 * of the original graph. Grouping is defined by specifying grouping keys for
 * vertices and edges, respectively, similarly as for GROUP BY in SQL.
 * <p>
 * Consider the following example:
 * <p>
 * Input graph:
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L})<br>
 * (1, "Person", {city: L})<br>
 * (2, "Person", {city: D})<br>
 * (3, "Person", {city: D})<br>
 * <p>
 * Edges:{(0,1), (1,0), (1,2), (2,1), (2,3), (3,2)}
 * <p>
 * Output graph (grouped on vertex property "city"):
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L, count: 2})
 * (2, "Person", {city: D, count: 2})
 * <p>
 * Edges:<br>
 * ((0, 0), {count: 2}) // 2 intra-edges in L<br>
 * ((2, 2), {count: 2}) // 2 intra-edges in L<br>
 * ((0, 2), {count: 1}) // 1 inter-edge from L to D<br>
 * ((2, 0), {count: 1}) // 1 inter-edge from D to L<br>
 * <p>
 * In addition to vertex properties, grouping is also possible on edge
 * properties, vertex- and edge labels as well as combinations of those.
 */
public abstract class Grouping implements UnaryGraphToGraphOperator {

  /**
   * Gradoop Flink configuration.
   */
  protected GradoopFlinkConfig config;
  /**
   * Used to group vertices.
   */
  private final List<String> vertexGroupingKeys;
  /**
   * True if vertices shall be grouped using their label.
   */
  private final boolean useVertexLabels;
  /**
   * Aggregate functions which are applied on super vertices.
   */
  private final List<PropertyValueAggregator> vertexAggregators;
  /**
   * Used to group edges.
   */
  private final List<String> edgeGroupingKeys;
  /**
   * True if edges shall be grouped using their label.
   */
  private final boolean useEdgeLabels;
  /**
   * Aggregate functions which are applied on super edges.
   */
  private final List<PropertyValueAggregator> edgeAggregators;

  /**
   * Creates grouping operator instance.
   *
   * @param vertexGroupingKeys  property keys to group vertices
   * @param useVertexLabels     group on vertex label true/false
   * @param vertexAggregators   aggregate functions for grouped vertices
   * @param edgeGroupingKeys    property keys to group edges
   * @param useEdgeLabels       group on edge label true/false
   * @param edgeAggregators     aggregate functions for grouped edges
   */
  Grouping(
    List<String> vertexGroupingKeys,
    boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators,
    List<String> edgeGroupingKeys,
    boolean useEdgeLabels,
    List<PropertyValueAggregator> edgeAggregators) {
    this.vertexGroupingKeys = checkNotNull(vertexGroupingKeys);
    this.useVertexLabels    = useVertexLabels;
    this.vertexAggregators  = vertexAggregators;
    this.edgeGroupingKeys   = checkNotNull(edgeGroupingKeys);
    this.useEdgeLabels      = useEdgeLabels;
    this.edgeAggregators    = edgeAggregators;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    LogicalGraph result;

    config = graph.getConfig();

    if (!useVertexProperties() &&
      !useEdgeProperties() &&
      !useVertexLabels() &&
      !useEdgeLabels()) {
      result = graph;
    } else {
      result = groupInternal(graph);
    }
    return result;
  }

  /**
   * Returns true if vertex properties shall be used for grouping.
   *
   * @return true iff vertex properties shall be used for grouping
   */
  protected boolean useVertexProperties() {
    return !vertexGroupingKeys.isEmpty();
  }

  /**
   * Returns vertex property keys which are used for grouping vertices.
   *
   * @return vertex property keys
   */
  protected List<String> getVertexGroupingKeys() {
    return vertexGroupingKeys;
  }

  /**
   * True, iff vertex labels shall be used for grouping.
   *
   * @return true, iff vertex labels shall be used for grouping
   */
  protected boolean useVertexLabels() {
    return useVertexLabels;
  }

  /**
   * Returns the aggregate functions which are applied on super vertices.
   *
   * @return vertex aggregate functions
   */
  protected List<PropertyValueAggregator> getVertexAggregators() {
    return vertexAggregators;
  }

  /**
   * Returns true if edge properties shall be used for grouping.
   *
   * @return true, iff edge properties shall be used for grouping
   */
  protected boolean useEdgeProperties() {
    return !edgeGroupingKeys.isEmpty();
  }

  /**
   * Returns edge property keys which are used for grouping edges.
   *
   * @return edge property keys
   */
  protected List<String> getEdgeGroupingKeys() {
    return edgeGroupingKeys;
  }

  /**
   * True, if edge labels shall be used for grouping.
   *
   * @return true, iff edge labels shall be used for grouping
   */
  protected boolean useEdgeLabels() {
    return useEdgeLabels;
  }

  /**
   * Returns the aggregate functions which shall be applied on super edges.
   *
   * @return edge aggregate functions
   */
  protected List<PropertyValueAggregator> getEdgeAggregators() {
    return edgeAggregators;
  }

  /**
   * Group vertices by either vertex label, vertex property or both.
   *
   * @param groupVertices dataset containing vertex representation for grouping
   * @return unsorted vertex grouping
   */
  protected UnsortedGrouping<VertexGroupItem> groupVertices(
    DataSet<VertexGroupItem> groupVertices) {
    UnsortedGrouping<VertexGroupItem> vertexGrouping;
    if (useVertexLabels() && useVertexProperties()) {
      vertexGrouping = groupVertices.groupBy(2, 3);
    } else if (useVertexLabels()) {
      vertexGrouping = groupVertices.groupBy(2);
    } else {
      vertexGrouping = groupVertices.groupBy(3);
    }
    return vertexGrouping;
  }

  /**
   * Groups edges based on the algorithm parameters.
   *
   * @param edges input graph edges
   * @return grouped edges
   */
  protected UnsortedGrouping<EdgeGroupItem> groupEdges(
    DataSet<EdgeGroupItem> edges) {
    UnsortedGrouping<EdgeGroupItem> groupedEdges;
    if (useEdgeProperties() && useEdgeLabels()) {
      groupedEdges = edges.groupBy(0, 1, 2, 3);
    } else if (useEdgeLabels()) {
      groupedEdges = edges.groupBy(0, 1, 2);
    } else if (useEdgeProperties()) {
      groupedEdges = edges.groupBy(0, 1, 3);
    } else {
      groupedEdges = edges.groupBy(0, 1);
    }
    return groupedEdges;
  }

  /**
   * Build super edges by joining them with vertices and their super vertex.
   *
   * @param graph                     input graph
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id
   *                                  and super vertex id
   * @return super edges
   */
  protected DataSet<Edge> buildSuperEdges(
    LogicalGraph graph,
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap) {

    DataSet<EdgeGroupItem> edges = graph.getEdges()
      // build edge group items
      .map(new BuildEdgeGroupItem(
        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators()))
      // join edges with vertex-group-map on source-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(0).equalTo(0)
      .with(new UpdateEdgeGroupItem(0))
      .withForwardedFieldsFirst("f1;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f0")
      // join result with vertex-group-map on target-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(1).equalTo(0)
      .with(new UpdateEdgeGroupItem(1))
      .withForwardedFieldsFirst("f0;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f1");

    // group + combine
    DataSet<EdgeGroupItem> combinedEdges = groupEdges(edges)
      .combineGroup(new CombineEdgeGroupItems(
        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators()));

    // group + reduce + build final edges
    return groupEdges(combinedEdges)
      .reduceGroup(new ReduceEdgeGroupItems(getEdgeGroupingKeys(),
        useEdgeLabels(),
        getEdgeAggregators(),
        config.getEdgeFactory()));
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return grouped output graph
   */
  protected abstract LogicalGraph groupInternal(
    LogicalGraph graph);

  /**
   * Used for building a grouping operator instance.
   */
  public static final class GroupingBuilder {

    /**
     * Grouping strategy
     */
    private GroupingStrategy strategy;

    /**
     * Property keys to group vertices.
     */
    private List<String> vertexGroupingKeys;

    /**
     * Property keys to group edges.
     */
    private List<String> edgeGroupingKeys;

    /**
     * True, iff vertex labels shall be considered.
     */
    private boolean useVertexLabel;

    /**
     * True, iff edge labels shall be considered.
     */
    private boolean useEdgeLabel;

    /**
     * Aggregate functions which will be applied on vertex properties.
     */
    private List<PropertyValueAggregator> vertexValueAggregators;

    /**
     * Aggregate functions which will be applied on edge properties.
     */
    private List<PropertyValueAggregator> edgeValueAggregators;

    /**
     * Creates a new grouping builder
     */
    public GroupingBuilder() {
      this.vertexGroupingKeys     = Lists.newArrayList();
      this.edgeGroupingKeys       = Lists.newArrayList();
      this.useVertexLabel         = false;
      this.useEdgeLabel           = false;
      this.vertexValueAggregators = Lists.newArrayList();
      this.edgeValueAggregators   = Lists.newArrayList();
    }

    /**
     * Set the grouping strategy.
     *
     * @param strategy grouping strategy
     * @see {@link GroupingStrategy}
     * @return this builder
     */
    public GroupingBuilder setStrategy(GroupingStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    /**
     * Adds a property key to the vertex grouping keys.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKey(String key) {
      checkNotNull(key);
      this.vertexGroupingKeys.add(key);
      return this;
    }

    /**
     * Adds a list of property keys to the vertex grouping keys.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKeys(
      List<String> keys) {
      checkNotNull(keys);
      this.vertexGroupingKeys.addAll(keys);
      return this;
    }

    /**
     * Adds a property key to the edge grouping keys.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKey(String key) {
      checkNotNull(key);
      this.edgeGroupingKeys.add(key);
      return this;
    }

    /**
     * Adds a list of property keys to the edge grouping keys.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKeys(
      List<String> keys) {
      checkNotNull(keys);
      this.edgeGroupingKeys.addAll(keys);
      return this;
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, iff vertex label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useVertexLabel(
      boolean useVertexLabel) {
      this.useVertexLabel = useVertexLabel;
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeLabel(
      boolean useEdgeLabel) {
      this.useEdgeLabel = useEdgeLabel;
      return this;
    }

    /**
     * Add an aggregate function which is applied on a group of vertices
     * represented by a single super vertex.
     *
     * @param aggregator vertex aggregator
     * @return this builder
     */
    public GroupingBuilder addVertexAggregator(
      PropertyValueAggregator aggregator) {
      checkNotNull(aggregator, "Aggregator must not be null");
      this.vertexValueAggregators.add(aggregator);
      return this;
    }

    /**
     * Add an aggregate function which is applied on a group of edges
     * represented by a single super edge.
     *
     * @param aggregator edge aggregator
     * @return this builder
     */
    public GroupingBuilder addEdgeAggregator(
      PropertyValueAggregator aggregator) {
      checkNotNull(aggregator, "Aggregator must not be null");
      this.edgeValueAggregators.add(aggregator);
      return this;
    }

    /**
     * Creates a new grouping operator instance based on the configured
     * parameters.
     *
     * @return grouping operator instance
     */
    public Grouping build() {
      if (vertexGroupingKeys.isEmpty() && !useVertexLabel) {
        throw new IllegalArgumentException(
          "Provide vertex key(s) and/or use vertex labels for grouping.");
      }

      Grouping groupingOperator;

      switch (strategy) {
      case GROUP_REDUCE:
        groupingOperator =
          new GroupingGroupReduce(vertexGroupingKeys, useVertexLabel,
            vertexValueAggregators, edgeGroupingKeys, useEdgeLabel,
            edgeValueAggregators);
        break;
      case GROUP_COMBINE:
        groupingOperator =
          new GroupingGroupCombine(vertexGroupingKeys, useVertexLabel,
            vertexValueAggregators, edgeGroupingKeys, useEdgeLabel,
            edgeValueAggregators);
        break;
      default:
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }

      return groupingOperator;
    }
  }
}
