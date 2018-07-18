/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.grouping;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.CombineEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.UpdateEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;
import java.util.Objects;

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
   * Used as property key to declare a label based grouping.
   *
   * See {@link LogicalGraph#groupBy(List, List, List, List, GroupingStrategy)}
   */
  public static final String LABEL_SYMBOL = ":label";
  /**
   * Used to verify if a grouping key is used for all vertices.
   */
  public static final String DEFAULT_VERTEX_LABEL_GROUP = ":defaultVertexLabelGroup";
  /**
   * Used to verify if a grouping key is used for all edges.
   */
  public static final String DEFAULT_EDGE_LABEL_GROUP = ":defaultEdgeLabelGroup";
  /**
   * Gradoop Flink configuration.
   */
  protected GradoopFlinkConfig config;
  /**
   * True if vertices shall be grouped using their label.
   */
  private final boolean useVertexLabels;
  /**
   * True if edges shall be grouped using their label.
   */
  private final boolean useEdgeLabels;
  /**
   * Stores grouping properties and aggregators for vertex labels.
   */
  private final List<LabelGroup> vertexLabelGroups;

  /**
   * Stores grouping properties and aggregators for edge labels.
   */
  private final List<LabelGroup> edgeLabelGroups;

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels   group on vertex label true/false
   * @param useEdgeLabels     group on edge label true/false
   * @param vertexLabelGroups stores grouping properties for vertex labels
   * @param edgeLabelGroups   stores grouping properties for edge labels
   */
  Grouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups) {
    this.useVertexLabels   = useVertexLabels;
    this.useEdgeLabels     = useEdgeLabels;
    this.vertexLabelGroups = vertexLabelGroups;
    this.edgeLabelGroups   = edgeLabelGroups;
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
    return !vertexLabelGroups.isEmpty();
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
   * Returns true if edge properties shall be used for grouping.
   *
   * @return true, iff edge properties shall be used for grouping
   */
  protected boolean useEdgeProperties() {
    return !edgeLabelGroups.isEmpty();
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
   * Returns tuple which contains the properties used for a specific vertex label.
   *
   * @return vertex label groups
   */
  public List<LabelGroup> getVertexLabelGroups() {
    return vertexLabelGroups;
  }

  /**
   * Returns tuple which contains the properties used for a specific edge label.
   *
   * @return edge label groups
   */
  public List<LabelGroup> getEdgeLabelGroups() {
    return edgeLabelGroups;
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
  protected UnsortedGrouping<EdgeGroupItem> groupEdges(DataSet<EdgeGroupItem> edges) {
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
      .flatMap(new BuildEdgeGroupItem(useEdgeLabels(), getEdgeLabelGroups()))
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
      .combineGroup(new CombineEdgeGroupItems(useEdgeLabels()));

    // group + reduce + build final edges
    return groupEdges(combinedEdges)
      .reduceGroup(new ReduceEdgeGroupItems(
        useEdgeLabels(),
        config.getEdgeFactory()));
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graphe
   * @return grouped output graph
   */
  protected abstract LogicalGraph groupInternal(LogicalGraph graph);

  /**
   * Used for building a grouping operator instance.
   */
  public static final class GroupingBuilder {

    /**
     * Grouping strategy
     */
    private GroupingStrategy strategy;
    /**
     * True, iff vertex labels shall be considered.
     */
    private boolean useVertexLabel;

    /**
     * True, iff edge labels shall be considered.
     */
    private boolean useEdgeLabel;
    /**
     * Stores grouping keys for a specific vertex label.
     */
    private List<LabelGroup> vertexLabelGroups;

    /**
     * Stores grouping keys for a specific edge label.
     */
    private List<LabelGroup> edgeLabelGroups;

    /**
     * Default vertex label group.
     */
    private LabelGroup defaultVertexLabelGroup;

    /**
     * Default edge label group.
     */
    private LabelGroup defaultEdgeLabelGroup;

    /**
     * List of all global vertex aggregate functions.
     */
    private List<PropertyValueAggregator> globalVertexAggregators;

    /**
     * List of all global edge aggregate functions.
     */
    private List<PropertyValueAggregator> globalEdgeAggregators;

    /**
     * Creates a new grouping builder
     */
    public GroupingBuilder() {
      this.useVertexLabel           = false;
      this.useEdgeLabel             = false;
      this.vertexLabelGroups        = Lists.newArrayList();
      this.edgeLabelGroups          = Lists.newArrayList();
      this.globalVertexAggregators  = Lists.newArrayList();
      this.globalEdgeAggregators    = Lists.newArrayList();
      this.defaultVertexLabelGroup  = new LabelGroup(
        Grouping.DEFAULT_VERTEX_LABEL_GROUP, GradoopConstants.DEFAULT_VERTEX_LABEL);
      this.defaultEdgeLabelGroup    = new LabelGroup(
        Grouping.DEFAULT_EDGE_LABEL_GROUP, GradoopConstants.DEFAULT_EDGE_LABEL);

      vertexLabelGroups.add(defaultVertexLabelGroup);
      edgeLabelGroups.add(defaultEdgeLabelGroup);
    }

    /**
     * Set the grouping strategy. See {@link GroupingStrategy}.
     *
     * @param strategy grouping strategy
     * @return this builder
     */
    public GroupingBuilder setStrategy(GroupingStrategy strategy) {
      Objects.requireNonNull(strategy);
      this.strategy = strategy;
      return this;
    }

    /**
     * Adds a property key to the vertex grouping keys for vertices which do not have a specific
     * label group.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKey(String key) {
      Objects.requireNonNull(key);
      if (key.equals(Grouping.LABEL_SYMBOL)) {
        useVertexLabel(true);
      } else {
        defaultVertexLabelGroup.addPropertyKey(key);
      }
      return this;
    }

    /**
     * Adds a list of property keys to the vertex grouping keys for vertices which do not have a
     * specific label group.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKeys(List<String> keys) {
      Objects.requireNonNull(keys);
      for (String key : keys) {
        this.addVertexGroupingKey(key);
      }
      return this;
    }

    /**
     * Adds a property key to the edge grouping keys for edges which do not have a specific
     * label group.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKey(String key) {
      Objects.requireNonNull(key);
      if (key.equals(Grouping.LABEL_SYMBOL)) {
        useEdgeLabel(true);
      } else {
        defaultEdgeLabelGroup.addPropertyKey(key);
      }
      return this;
    }

    /**
     * Adds a list of property keys to the edge grouping keys for edges  which do not have a
     * specific label group.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKeys(List<String> keys) {
      Objects.requireNonNull(keys);
      for (String key : keys) {
        this.addEdgeGroupingKey(key);
      }
      return this;
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label vertex label
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      List<String> groupingKeys) {
      return addVertexLabelGroup(label, label, groupingKeys, Lists.newArrayList());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label vertex label
     * @param groupingKeys keys used for grouping
     * @param aggregators vertex aggregators
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      return addVertexLabelGroup(label, label, groupingKeys, aggregators);
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label vertex label
     * @param superVertexLabel label of the group and therefore of the new super vertex
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      String superVertexLabel,
      List<String> groupingKeys) {
      return addVertexLabelGroup(label, superVertexLabel, groupingKeys, Lists.newArrayList());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label vertex label
     * @param superVertexLabel label of the group and therefore of the new super vertex
     * @param groupingKeys keys used for grouping
     * @param aggregators vertex aggregators
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      String superVertexLabel,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      vertexLabelGroups.add(new LabelGroup(label, superVertexLabel, groupingKeys, aggregators));
      return this;
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      List<String> groupingKeys) {
      return addEdgeLabelGroup(label, label, groupingKeys, Lists.newArrayList());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param groupingKeys keys used for grouping
     * @param aggregators edge aggregators
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      return addEdgeLabelGroup(label, label, groupingKeys, aggregators);
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param superEdgeLabel label of the group and therefore of the new super edge
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      String superEdgeLabel,
      List<String> groupingKeys) {
      return addEdgeLabelGroup(label, superEdgeLabel, groupingKeys, Lists.newArrayList());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param superEdgeLabel label of the group and therefore of the new super edge
     * @param groupingKeys keys used for grouping
     * @param aggregators edge aggregators
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      String superEdgeLabel,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      edgeLabelGroups.add(new LabelGroup(label, superEdgeLabel, groupingKeys, aggregators));
      return this;
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, iff vertex label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useVertexLabel(boolean useVertexLabel) {
      this.useVertexLabel = useVertexLabel;
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeLabel(boolean useEdgeLabel) {
      this.useEdgeLabel = useEdgeLabel;
      return this;
    }

    /**
     * Add an aggregate function which is applied on all vertices represented by a single super
     * vertex which do not have a specific label group.
     *
     * @param aggregator vertex aggregator
     * @return this builder
     */
    public GroupingBuilder addVertexAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      defaultVertexLabelGroup.addAggregator(aggregator);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all vertices represented by a single super
     * vertex.
     *
     * @param aggregator vertex aggregator
     * @return this builder
     */
    public GroupingBuilder addGlobalVertexAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      globalVertexAggregators.add(aggregator);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge.
     *
     * @param aggregator edge aggregator
     * @return this builder
     */
    public GroupingBuilder addGlobalEdgeAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      globalEdgeAggregators.add(aggregator);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge
     * which do not have a specific label group.
     *
     * @param aggregator edge aggregator
     * @return this builder
     */
    public GroupingBuilder addEdgeAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      defaultEdgeLabelGroup.addAggregator(aggregator);
      return this;
    }

    /**
     * Creates a new grouping operator instance based on the configured
     * parameters.
     *
     * @return grouping operator instance
     */
    public Grouping build() {
      if (vertexLabelGroups.isEmpty() && !useVertexLabel) {
        throw new IllegalArgumentException(
          "Provide vertex key(s) and/or use vertex labels for grouping.");
      }

      // adding the global aggregators to the associated label groups
      for (LabelGroup vertexLabelGroup : vertexLabelGroups) {
        for (PropertyValueAggregator vertexAggregator : globalVertexAggregators) {
          vertexLabelGroup.addAggregator(vertexAggregator);
        }
      }

      for (LabelGroup edgeLabelGroup : edgeLabelGroups) {
        for (PropertyValueAggregator edgeAggregator : globalEdgeAggregators) {
          edgeLabelGroup.addAggregator(edgeAggregator);
        }
      }

      Grouping groupingOperator;

      switch (strategy) {
      case GROUP_REDUCE:
        groupingOperator = new GroupingGroupReduce(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups);
        break;
      case GROUP_COMBINE:
        groupingOperator = new GroupingGroupCombine(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups);
        break;
      default:
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }

      return groupingOperator;
    }
  }
}
