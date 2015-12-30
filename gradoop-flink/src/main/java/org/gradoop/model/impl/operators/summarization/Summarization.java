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

package org.gradoop.model.impl.operators.summarization;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.operators.summarization.functions.BuildEdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.CombineEdgeGroupItems;
import org.gradoop.model.impl.operators.summarization.functions.ReduceEdgeGroupItems;
import org.gradoop.model.impl.operators.summarization.functions.UpdateEdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The summarization operator determines a structural grouping of similar
 * vertices and edges to condense a graph and thus help to uncover insights
 * about patterns hidden in the graph.
 * <p>
 * The graph summarization operator represents every vertex group by a single
 * vertex in the summarized graph; edges between vertices in the summary graph
 * represent a group of edges between the vertex group members of the
 * original graph. Summarization is defined by specifying grouping keys for
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
 * Output graph (summarized on vertex property "city"):
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
 * In addition to vertex properties, summarization is also possible on edge
 * properties, vertex- and edge labels as well as combinations of those.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public abstract class Summarization<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Gradoop Flink configuration.
   */
  protected GradoopFlinkConfig<G, V, E> config;
  /**
   * Used to summarize vertices.
   */
  private final List<String> vertexGroupingKeys;
  /**
   * True if vertices shall be summarized using their label.
   */
  private final boolean useVertexLabels;
  /**
   * Aggregate function which is applied on summarized vertices.
   */
  private final PropertyValueAggregator vertexAggregator;
  /**
   * Used to summarize edges.
   */
  private final List<String> edgeGroupingKeys;
  /**
   * True if edges shall be summarized using their label.
   */
  private final boolean useEdgeLabels;
  /**
   * Aggregate function which is applied on summarized edges.
   */
  private final PropertyValueAggregator edgeAggregator;

  /**
   * Creates summarization.
   *
   * @param vertexGroupingKeys  property keys to summarize vertices
   * @param useVertexLabels     summarize on vertex label true/false
   * @param vertexAggregator    aggregate function for summarized vertices
   * @param edgeGroupingKeys    property keys to summarize edges
   * @param useEdgeLabels       summarize on edge label true/false
   * @param edgeAggregator      aggregate function for summarized edges
   */
  Summarization(List<String> vertexGroupingKeys, boolean useVertexLabels,
    PropertyValueAggregator vertexAggregator, List<String> edgeGroupingKeys,
    boolean useEdgeLabels, PropertyValueAggregator edgeAggregator) {
    this.vertexGroupingKeys = checkNotNull(vertexGroupingKeys);
    this.useVertexLabels    = useVertexLabels;
    this.vertexAggregator   = vertexAggregator;
    this.edgeGroupingKeys   = checkNotNull(edgeGroupingKeys);
    this.useEdgeLabels      = useEdgeLabels;
    this.edgeAggregator     = edgeAggregator;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    LogicalGraph<G, V, E> result;

    config = graph.getConfig();

    if (!useVertexProperties() &&
      !useEdgeProperties() &&
      !useVertexLabels() &&
      !useEdgeLabels()) {
      result = graph;
    } else {
      result = summarizeInternal(graph);
    }
    return result;
  }

  /**
   * Returns true if the vertex property shall be used for summarization.
   *
   * @return true if vertex property shall be used for summarization, false
   * otherwise
   */
  protected boolean useVertexProperties() {
    return !vertexGroupingKeys.isEmpty();
  }

  /**
   * Vertex property key to use for summarizing vertices.
   *
   * @return vertex property key
   */
  protected List<String> getVertexGroupingKeys() {
    return vertexGroupingKeys;
  }

  /**
   * True, if vertex labels shall be used for summarization.
   *
   * @return true, if vertex labels shall be used for summarization, false
   * otherwise
   */
  protected boolean useVertexLabels() {
    return useVertexLabels;
  }

  /**
   * Returns the aggregate function which shall be applied on vertex groups.
   *
   * @return vertex aggregate function
   */
  protected PropertyValueAggregator getVertexAggregator() {
    return vertexAggregator;
  }

  /**
   * Returns true if the edge property shall be used for summarization.
   *
   * @return true if edge property shall be used for summarization, false
   * otherwise
   */
  protected boolean useEdgeProperties() {
    return !edgeGroupingKeys.isEmpty();
  }

  /**
   * Edge property key to use for summarizing edges.
   *
   * @return edge property key
   */
  protected List<String> getEdgeGroupingKeys() {
    return edgeGroupingKeys;
  }

  /**
   * True, if edge labels shall be used for summarization.
   *
   * @return true, if edge labels shall be used for summarization, false
   * otherwise
   */
  protected boolean useEdgeLabels() {
    return useEdgeLabels;
  }

  /**
   * Returns the aggregate function which shall be applied on edge groups.
   *
   * @return edge aggregate function
   */
  protected PropertyValueAggregator getEdgeAggregator() {
    return edgeAggregator;
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
   * Build summarized edges by joining them with vertices and their group
   * representative.
   *
   * @param graph                     input graph
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id
   *                                  and group representative
   * @return summarized edges
   */
  protected DataSet<E> buildSummarizedEdges(
    LogicalGraph<G, V, E> graph,
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap) {

    // join edges with vertex-group-map on vertex-id == edge-source-id
    DataSet<EdgeGroupItem> edges = graph.getEdges()
      .join(vertexToRepresentativeMap)
      .where(new SourceId<E>()).equalTo(0)
      // project edges to necessary information
      .with(new BuildEdgeGroupItem<E>(
        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregator()))
      // join result with vertex-group-map on edge-target-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(1).equalTo(0)
      .with(new UpdateEdgeGroupItem());

    // group + combine
    DataSet<EdgeGroupItem> combinedEdges = groupEdges(edges)
      .combineGroup(new CombineEdgeGroupItems(
        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregator()));

    // group + reduce + build final edges
    return groupEdges(combinedEdges)
      .reduceGroup(new ReduceEdgeGroupItems<>(getEdgeGroupingKeys(),
        useEdgeLabels(),
        getEdgeAggregator(),
        config.getEdgeFactory()));
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return summarized output graph
   */
  protected abstract LogicalGraph<G, V, E> summarizeInternal(
    LogicalGraph<G, V, E> graph);


  /**
   * Used for building a summarization instance.
   *
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   */
  public static final class SummarizationBuilder<
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> {

    /**
     * Summarization strategy
     */
    private SummarizationStrategy strategy;

    /**
     * Property keys to group vertices.
     */
    private List<String> vertexGroupingKeys;

    /**
     * Property keys to group edges.
     */
    private List<String> edgeGroupingKeys;

    /**
     * True, if vertex labels shall be considered in summarization.
     */
    private boolean useVertexLabel;

    /**
     * True, if edge labels shall be considered in summarization.
     */
    private boolean useEdgeLabel;

    /**
     * Aggregator which will be applied on vertex properties.
     */
    private PropertyValueAggregator vertexValueAggregator;

    /**
     * Aggregator which will be applied on edge properties.
     */
    private PropertyValueAggregator edgeValueAggregator;

    /**
     * Creates a new summarization builder
     */
    public SummarizationBuilder() {
      this.vertexGroupingKeys     = Lists.newArrayList();
      this.edgeGroupingKeys       = Lists.newArrayList();
      this.useVertexLabel         = false;
      this.useEdgeLabel           = false;
      this.vertexValueAggregator  = null;
      this.edgeValueAggregator    = null;
    }

    /**
     * Set the summarization strategy.
     *
     * @param strategy summarization strategy
     * @see {@link SummarizationStrategy}
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> setStrategy(
      SummarizationStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    /**
     * Adds a property key to the vertex grouping keys.
     *
     * @param key property key
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> addVertexGroupingKey(String key) {
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
    public SummarizationBuilder<G, V, E> addVertexGroupingKeys(
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
    public SummarizationBuilder<G, V, E> addEdgeGroupingKey(String key) {
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
    public SummarizationBuilder<G, V, E> addEdgeGroupingKeys(
      List<String> keys) {
      checkNotNull(keys);
      this.edgeGroupingKeys.addAll(keys);
      return this;
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, if label shall be used, false otherwise
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> useVertexLabel(
      boolean useVertexLabel) {
      this.useVertexLabel = useVertexLabel;
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, if label shall be used, false otherwise
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> useEdgeLabel(
      boolean useEdgeLabel) {
      this.useEdgeLabel = useEdgeLabel;
      return this;
    }

    /**
     * Used to compute an aggregate for each vertex group.
     *
     * @param vertexValueAggregator aggregator
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> setVertexValueAggregator(
      PropertyValueAggregator vertexValueAggregator) {
      this.vertexValueAggregator = vertexValueAggregator;
      return this;
    }

    /**
     * Used to compute an aggregate for each edge group.
     *
     * @param edgeValueAggregator aggregator
     * @return this builder
     */
    public SummarizationBuilder<G, V, E> setEdgeValueAggregator(
      PropertyValueAggregator edgeValueAggregator) {
      this.edgeValueAggregator = edgeValueAggregator;
      return this;
    }

    /**
     * Creates a new summarization instance based on the configured parameters.
     *
     * @return summarization
     */
    public Summarization<G, V, E> build() {
      if (vertexGroupingKeys.isEmpty() && !useVertexLabel) {
        throw new IllegalArgumentException(
          "Provide vertex key(s) and/or use vertex labels for summarization.");
      }

      if (strategy == SummarizationStrategy.GROUP_MAP) {
        return new SummarizationGroupMap<>(
          vertexGroupingKeys, useVertexLabel, vertexValueAggregator,
          edgeGroupingKeys, useEdgeLabel, edgeValueAggregator);
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    }
  }
}
