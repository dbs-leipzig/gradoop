/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.operators.SetInTupleKeySelector;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.CombineEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.UpdateEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;
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
   * Used as property key to declare a source based edge centric grouping.
   */
  public static final String SOURCE_SYMBOL = ":source";
  /**
   * Used as property key to declare a target based edge centric grouping.
   */
  public static final String TARGET_SYMBOL = ":target";
  /**
   * Defines the default label separator between aggregated labels in edge centric grouping.
   */
  public static final String LABEL_SEPARATOR = "";
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
   * Group edges by either vertex label, vertex property or both and additionally source specific,
   * target specific or both.
   *
   * @param groupSuperEdges dataset containing edge representation for grouping
   * @param sourceSpecific true if the source vertex shall be considered for grouping
   * @param targetSpecific true if the target vertex shall be considered for grouping
   * @return unsorted edge grouping
   */
  protected UnsortedGrouping<SuperEdgeGroupItem> groupSuperEdges(
    DataSet<SuperEdgeGroupItem> groupSuperEdges, Boolean sourceSpecific, Boolean targetSpecific) {
    UnsortedGrouping<SuperEdgeGroupItem> edgeGrouping;

    if (useEdgeLabels() && useEdgeProperties()) {
      if (sourceSpecific && targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 3, 4, 5));
      } else if (sourceSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 4, 5));
      } else if (targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(3, 4, 5));
      } else {
        edgeGrouping = groupSuperEdges.groupBy(4, 5);
      }
    } else if (useEdgeLabels()) {
      if (sourceSpecific && targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 3, 4));
      } else if (sourceSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 4));
      } else if (targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(3, 4));
      } else {
        edgeGrouping = groupSuperEdges.groupBy(4);
      }
    } else {
      if (sourceSpecific && targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 3, 5));
      } else if (sourceSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(2, 5));
      } else if (targetSpecific) {
        edgeGrouping = groupSuperEdges.groupBy(
          new SetInTupleKeySelector<SuperEdgeGroupItem>(3, 5));
      } else {
        edgeGrouping = groupSuperEdges.groupBy(5);
      }
    }
    return edgeGrouping;
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
   * @param graph input graph
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
     * Centrical grouping strategy.
     */
    private GroupingStrategy centricalStrategy;

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
     * True, iff edge's source shall be considered.
     */
    private boolean useEdgeSource;

    /**
     * True, iff edge's target shall be considered.
     */
    private boolean useEdgeTarget;
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
      this.useEdgeSource            = false;
      this.useEdgeTarget            = false;
      this.vertexLabelGroups        = Lists.newArrayList();
      this.edgeLabelGroups          = Lists.newArrayList();
      this.globalVertexAggregators  = Lists.newArrayList();
      this.globalEdgeAggregators    = Lists.newArrayList();
      this.centricalStrategy        = GroupingStrategy.VERTEX_CENTRIC;
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
     * Set the grouping strategy. See {@link GroupingStrategy}.
     *
     * @param centricalStrategy grouping strategy
     * @return this builder
     */
    public GroupingBuilder setCentricalStrategy(GroupingStrategy centricalStrategy) {
      Objects.requireNonNull(centricalStrategy);
      this.centricalStrategy = centricalStrategy;
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
      } else if (key.equals(Grouping.SOURCE_SYMBOL)) {
        useEdgeSource(true);
      } else if (key.equals(Grouping.TARGET_SYMBOL)) {
        useEdgeTarget(true);
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
     * @param labelGroup label group
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(LabelGroup labelGroup) {
      vertexLabelGroups.add(labelGroup);
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
      String label, String superVertexLabel,
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
      String label, String superVertexLabel,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      vertexLabelGroups.add(new LabelGroup(label, superVertexLabel, groupingKeys, aggregators));
      return this;
    }

    /**
     * Adds an edge label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param labelGroup
     * @return
     */
    public GroupingBuilder addEdgeLabelGroup(LabelGroup labelGroup) {
      edgeLabelGroups.add(labelGroup);
      return this;
    }
    /**
     * Adds an edge label group which defines the grouping keys for a specific label.
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
     * Adds an edge label group which defines the grouping keys and the aggregators for a
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
     * Adds an edge label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param superEdgeLabel label of the group and therefore of the new super vertex
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label, String superEdgeLabel,
      List<String> groupingKeys) {
      return addEdgeLabelGroup(label, superEdgeLabel, groupingKeys, Lists.newArrayList());
    }

    /**
     * Adds an edge label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label edge label
     * @param superEdgeLabel label of the group and therefore of the new super vertex
     * @param groupingKeys keys used for grouping
     * @param aggregators edge aggregators
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label, String superEdgeLabel,
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
     * Define, if the edge's source shall be used for grouping edges.
     *
     * @param useEdgeSource true, iff edge's source shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeSource(boolean useEdgeSource) {
      this.useEdgeSource = useEdgeSource;
      return this;
    }

    /**
     * Define, if the edge's target shall be used for grouping edges.
     *
     * @param useEdgeTarget true, iff edge's target shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeTarget(boolean useEdgeTarget) {
      this.useEdgeTarget = useEdgeTarget;
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
      //new grouping approach is edge centric
      if (centricalStrategy.equals(GroupingStrategy.EDGE_CENTRIC)) {
        if (edgeLabelGroups.isEmpty() && !useEdgeLabel) {
          throw new IllegalArgumentException(
            "Provide edge key(s) and/or use edge labels for grouping.");
        }
        if (vertexLabelGroups.size() > 1) {
          throw new IllegalArgumentException(
            "Using vertex label groups is not allowed with edge centric grouping.");
        }
        //old grouping or new grouping and vertex centric
      } else {
        if (vertexLabelGroups.isEmpty() && !useVertexLabel) {
          throw new IllegalArgumentException(
            "Provide vertex key(s) and/or use vertex labels for grouping.");
        }
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

      switch (centricalStrategy) {
      case VERTEX_CENTRIC:
        groupingOperator = new VertexCentricalGrouping(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups, strategy);
        break;
      case EDGE_CENTRIC:
        groupingOperator = new EdgeCentricalGrouping(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups, strategy,
          useEdgeSource, useEdgeTarget);
        break;
      default:
        throw new IllegalArgumentException("Unsupported centrical strategy: " + centricalStrategy);
      }

      return groupingOperator;
    }
  }
}
