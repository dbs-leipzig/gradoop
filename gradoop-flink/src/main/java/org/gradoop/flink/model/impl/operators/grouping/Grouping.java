/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.filters.Not;
import org.gradoop.flink.model.impl.functions.filters.Or;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.CombineEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.SubtractCoGroupFunction;
import org.gradoop.flink.model.impl.operators.grouping.functions.UpdateEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.VertexSuperVertexIdentity;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.verify.Verify;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

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
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 */
public abstract class Grouping<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>>  implements UnaryBaseGraphToBaseGraphOperator<LG> {
  /**
   * Used as property key to declare a label based grouping.
   * <p>
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
   * True: vertices that are not member of any labelGroup are converted as is to supervertices.
   * False: vertices that are not member of any labelGroup will be collapsed into a single group/
   * supervertice.
   */
  private final boolean retainVerticesWithoutGroups;

  /**
   * Returns true, if a vertex is not a member of any labelGroup.
   */
  private final FilterFunction<V> vertexInNoGroupFilter;

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels             group on vertex label true/false
   * @param useEdgeLabels               group on edge label true/false
   * @param vertexLabelGroups           stores grouping properties for vertex labels
   * @param edgeLabelGroups             stores grouping properties for edge labels
   * @param retainVerticesWithoutGroups convert vertices without labels to supervertices (only
   *                                    applies when
   *                                    grouping by labels)
   */
  @SuppressWarnings("unchecked")
  Grouping(boolean useVertexLabels, boolean useEdgeLabels, List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups, boolean retainVerticesWithoutGroups) {
    this.useVertexLabels = useVertexLabels;
    this.useEdgeLabels = useEdgeLabels;
    this.vertexLabelGroups = vertexLabelGroups;
    this.edgeLabelGroups = edgeLabelGroups;
    this.retainVerticesWithoutGroups = retainVerticesWithoutGroups;

    FilterFunction[] isVertexInLabelGroupFunctions = getVertexLabelGroups()
      .parallelStream()
      .map(this::getIsVertexMemberOfLabelGroupFilter)
      .collect(Collectors.toList())
      .toArray(new FilterFunction[getVertexLabelGroups().size()]);

    vertexInNoGroupFilter = new Not<>(new Or<V>(isVertexInLabelGroupFunctions));
  }

  @Override
  public LG execute(LG graph) {
    LG result;

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
   * True, iff vertices without labels will be converted to individual groups/ supervertices.
   * False, iff vertices without labels will be collapsed into a single group/ supervertice.
   *
   * @return true, iff vertices will be converted
   */
  protected boolean isRetainingVerticesWithoutGroups() {
    return retainVerticesWithoutGroups;
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
   * @param factory factory
   * @param edgesToGroup input edgesToGroup
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id and super vertex id
   * @return super edges
   */
  protected DataSet<E> buildSuperEdges(
    BaseGraphFactory<G, V, E, LG> factory,
    DataSet<E> edgesToGroup,
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap) {

    DataSet<EdgeGroupItem> edges = edgesToGroup
      // build edge group items
      .flatMap(new BuildEdgeGroupItem<>(useEdgeLabels(), getEdgeLabelGroups()))
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
      .reduceGroup(new ReduceEdgeGroupItems<>(
        useEdgeLabels(),
        factory.getEdgeFactory()));
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graphe
   * @return grouped output graph
   */
  protected abstract LG groupInternal(LG graph);

  /**
   * Returns a {@link FilterFunction<V>} that checks if a vertex is member of a
   * {@link LabelGroup}.
   *
   * @param group to check
   * @return function
   */
  private FilterFunction<V> getIsVertexMemberOfLabelGroupFilter(LabelGroup group) {

    /*
     * Returns true, if a vertex exhibits all properties of a labelGroup.
     * Also returns true, if grouped by properties (propertyKeys) are empty.
     */
    BiFunction<LabelGroup, V, Boolean> hasVertexAllPropertiesOfGroup =
      (BiFunction<LabelGroup, V, Boolean> & Serializable) (labelGroup, vertex) ->
        group.getPropertyKeys()
          .parallelStream()
          .allMatch(vertex::hasProperty);

    boolean groupingByLabels = useVertexLabels();

    // Default case (parameter group is not label specific)
    if (group.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP)) {

      return (FilterFunction<V>) vertex -> {

        if (groupingByLabels) {

          // if the vertex's label is empty,
          // a) and the group by properties are empty => vertex is not member of the group
          // b) and group by properties are not empty => vertex can be member of the group
          if (vertex.getLabel().isEmpty()) {
            return !group.getPropertyKeys().isEmpty() && hasVertexAllPropertiesOfGroup
              .apply(group, vertex);
          } else {
            return hasVertexAllPropertiesOfGroup.apply(group, vertex);
          }

        } else {

          // if we are not grouping by labels, we need to check if the vertex has all
          // properties grouped by
          // if there is no grouped by property, no vertex should be part of the group

          return !group.getPropertyKeys().isEmpty() && hasVertexAllPropertiesOfGroup
            .apply(group, vertex);
        }
      };
    }

    // Label specific grouping case:
    // a vertex is a member of a label specific group, if the labels match and if the vertex has
    // all of the required properties
    return (FilterFunction<V>) vertex -> vertex.getLabel().equals(group.getGroupingLabel()) &&
      hasVertexAllPropertiesOfGroup.apply(group, vertex);
  }

  /**
   * Returns a verified subgraph that only includes vertices that are not member of any labelGroups.
   *
   * @param graph to filter
   * @return subgraph
   */
  LG getRetainedVerticesSubgraph(LG graph) {

    LG filtered = new Subgraph<G, V, E, LG>(vertexInNoGroupFilter, new True<>(),
      Subgraph.Strategy.VERTEX_INDUCED)
      .execute(graph);
    // TODO after next update: is verify still necesarry?

    return new Verify<G, V, E, LG>().execute(filtered);
  }

  /**
   * Returns a {@link DataSet<V>} that contains all vertices that belong to a
   * {@link LabelGroup}.
   *
   * @param vertices to check
   * @return filtered vertices
   */
  DataSet<V> getVerticesToGroup(DataSet<V> vertices) {
    return vertices.filter(new Not<>(vertexInNoGroupFilter));
  }

  /**
   * Is used when {@link Grouping#retainVerticesWithoutGroups} is set.
   * Edges between retained vertices are not grouped, but converted as they are.
   * So set of edges to be grouped needs to be reduced accordingly.
   * Computes: {@code graph.getEdges() - retainedVerticesSubgraph.getEdges()}
   *
   * @param graph to be grouped
   * @param retainedVerticesSubgraph graph that contains all retained vertices
   * @return reduced set of edges
   */
  DataSet<E> subtractRetainedEdgesFromDefaultGraph(LG graph,
    LG retainedVerticesSubgraph) {

    return graph.getEdges()
      .coGroup(retainedVerticesSubgraph.getEdges())
      .where("sourceId", "targetId")
      .equalTo("sourceId", "targetId")
      //.with(new SubtractCoGroupFunction())
      .with(new SubtractCoGroupFunction<>());
  }

  /**
   * Is used when {@link Grouping#retainVerticesWithoutGroups} is set.
   * To add support for grouped edges between retained vertices and supervertices,
   * retained vertices are singleton groups and are their group representatives themself.
   *
   * @param vertexToRepresentativeMap vertex representative map to add to
   * @param retainedVerticesSubgraph graph that contains only retained vertices
   * @return updated vertexToRepresentativeMap
   */
  DataSet<VertexWithSuperVertex> updateVertexRepresentativesWithUngroupedVertices(
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap,
    LG retainedVerticesSubgraph) {
    DataSet<VertexWithSuperVertex> verticesRepresentatives =
      retainedVerticesSubgraph.getVertices()
        .map(new VertexSuperVertexIdentity<>());

    vertexToRepresentativeMap = vertexToRepresentativeMap.union(verticesRepresentatives);
    return vertexToRepresentativeMap;
  }

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
    private List<AggregateFunction> globalVertexAggregateFunctions;

    /**
     * List of all global edge aggregate functions.
     */
    private List<AggregateFunction> globalEdgeAggregateFunctions;

    /**
     * True, iff vertices without labels will be converted to individual groups/ supervertices.
     * False, iff vertices without labels will be collapsed into a single group/ supervertice.
     */
    private boolean retainVerticesWithoutGroups;

    /**
     * Creates a new grouping builder
     */
    public GroupingBuilder() {
      this.useVertexLabel = false;
      this.useEdgeLabel = false;
      this.vertexLabelGroups = new ArrayList<>();
      this.edgeLabelGroups = new ArrayList<>();
      this.globalVertexAggregateFunctions = new ArrayList<>();
      this.globalEdgeAggregateFunctions = new ArrayList<>();
      this.defaultVertexLabelGroup =
        new LabelGroup(Grouping.DEFAULT_VERTEX_LABEL_GROUP, GradoopConstants.DEFAULT_VERTEX_LABEL);
      this.defaultEdgeLabelGroup =
        new LabelGroup(Grouping.DEFAULT_EDGE_LABEL_GROUP, GradoopConstants.DEFAULT_EDGE_LABEL);

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
     * True: vertices that are not member of any labelGroup are converted as is to supervertices.
     * False: vertices that are not member of any labelGroup will be collapsed into a single group/
     * supervertice.
     *
     * @param retainVerticesWithoutGroups flag
     * @return this builder
     */
    GroupingBuilder setRetainVerticesWithoutGroups(boolean retainVerticesWithoutGroups) {
      this.retainVerticesWithoutGroups = retainVerticesWithoutGroups;
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
     * @param label        vertex label
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      List<String> groupingKeys) {
      return addVertexLabelGroup(label, label, groupingKeys);
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregate functions for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label              vertex label
     * @param groupingKeys       keys used for grouping
     * @param aggregateFunctions vertex aggregate functions
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      List<String> groupingKeys,
      List<AggregateFunction> aggregateFunctions) {
      return addVertexLabelGroup(label, label, groupingKeys, aggregateFunctions);
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label            vertex label
     * @param superVertexLabel label of the group and therefore of the new super vertex
     * @param groupingKeys     keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      String superVertexLabel,
      List<String> groupingKeys) {
      return addVertexLabelGroup(label, superVertexLabel, groupingKeys, new ArrayList<>());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregate functions for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label              vertex label
     * @param superVertexLabel   label of the group and therefore of the new super vertex
     * @param groupingKeys       keys used for grouping
     * @param aggregateFunctions vertex aggregate functions
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String label,
      String superVertexLabel,
      List<String> groupingKeys,
      List<AggregateFunction> aggregateFunctions) {
      vertexLabelGroups.add(new LabelGroup(label, superVertexLabel, groupingKeys,
        aggregateFunctions));
      return this;
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label        edge label
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      List<String> groupingKeys) {
      return addEdgeLabelGroup(label, label, groupingKeys);
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregate functions for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label              edge label
     * @param groupingKeys       keys used for grouping
     * @param aggregateFunctions edge aggregate functions
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      List<String> groupingKeys,
      List<AggregateFunction> aggregateFunctions) {
      return addEdgeLabelGroup(label, label, groupingKeys, aggregateFunctions);
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param label          edge label
     * @param superEdgeLabel label of the group and therefore of the new super edge
     * @param groupingKeys   keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      String superEdgeLabel,
      List<String> groupingKeys) {
      return addEdgeLabelGroup(label, superEdgeLabel, groupingKeys, new ArrayList<>());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregate functions for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param label              edge label
     * @param superEdgeLabel     label of the group and therefore of the new super edge
     * @param groupingKeys       keys used for grouping
     * @param aggregateFunctions edge aggregate functions
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String label,
      String superEdgeLabel,
      List<String> groupingKeys,
      List<AggregateFunction> aggregateFunctions) {
      edgeLabelGroups.add(new LabelGroup(label, superEdgeLabel, groupingKeys, aggregateFunctions));
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
     * @param aggregateFunction vertex aggregate function
     * @return this builder
     */
    public GroupingBuilder addVertexAggregateFunction(AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      defaultVertexLabelGroup.addAggregateFunction(aggregateFunction);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all vertices represented by a single super
     * vertex.
     *
     * @param aggregateFunction vertex aggregate function
     * @return this builder
     */
    public GroupingBuilder addGlobalVertexAggregateFunction(
      AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      globalVertexAggregateFunctions.add(aggregateFunction);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge.
     *
     * @param aggregateFunction edge aggregate function
     * @return this builder
     */
    public GroupingBuilder addGlobalEdgeAggregateFunction(AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      globalEdgeAggregateFunctions.add(aggregateFunction);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge
     * which do not have a specific label group.
     *
     * @param aggregateFunction edge aggregate function
     * @return this builder
     */
    public GroupingBuilder addEdgeAggregateFunction(AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      defaultEdgeLabelGroup.addAggregateFunction(aggregateFunction);
      return this;
    }

    /**
     * Creates a new grouping operator instance based on the configured
     * parameters.
     *
     * @param <G> The graph head type.
     * @param <V> The vertex type.
     * @param <E> The edge type.
     * @param <LG> The type of the graph.
     * @return grouping operator instance
     */
    public <
      G extends EPGMGraphHead,
      V extends EPGMVertex,
      E extends EPGMEdge,
      LG extends BaseGraph<G, V, E, LG>> Grouping<G, V, E, LG> build() {
      if (vertexLabelGroups.isEmpty() && !useVertexLabel) {
        throw new IllegalArgumentException(
          "Provide vertex key(s) and/or use vertex labels for grouping.");
      }

      // adding the global aggregators to the associated label groups
      for (LabelGroup vertexLabelGroup : vertexLabelGroups) {
        for (AggregateFunction aggregateFunction : globalVertexAggregateFunctions) {
          vertexLabelGroup.addAggregateFunction(aggregateFunction);
        }
      }

      for (LabelGroup edgeLabelGroup : edgeLabelGroups) {
        for (AggregateFunction aggregateFunction : globalEdgeAggregateFunctions) {
          edgeLabelGroup.addAggregateFunction(aggregateFunction);
        }
      }

      Grouping<G, V, E, LG> groupingOperator;

      switch (strategy) {
      case GROUP_REDUCE:
        groupingOperator = new GroupingGroupReduce<>(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups,
          retainVerticesWithoutGroups);
        break;
      case GROUP_COMBINE:
        groupingOperator = new GroupingGroupCombine<>(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups,
          retainVerticesWithoutGroups);
        break;
      default:
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }

      return groupingOperator;
    }
  }
}
