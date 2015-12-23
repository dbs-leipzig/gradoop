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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.operators.summarization.functions.UpdateSourceId;
import org.gradoop.model.impl.operators.summarization.functions.BuildSummarizedEdge;
import org.gradoop.model.impl.operators.summarization.functions.UpdateTargetId;
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
   * EPGMProperty key to store the number of summarized entities in a group.
   */
  public static final String COUNT_PROPERTY_KEY = "count";

  /**
   * Gradoop Flink configuration.
   */
  protected GradoopFlinkConfig<G, V, E> config;
  /**
   * Used to summarize vertices.
   */
  private final List<String> vertexGroupingKeys;
  /**
   * Used to summarize edges.
   */
  private final List<String> edgeGroupingKeys;
  /**
   * True if vertices shall be summarized using their label.
   */
  private final boolean useVertexLabels;
  /**
   * True if edges shall be summarized using their label.
   */
  private final boolean useEdgeLabels;

  /**
   * Creates summarization.
   *
   * @param vertexGroupingKeys  property keys to summarize vertices
   * @param edgeGroupingKeys    property keys to summarize edges
   * @param useVertexLabels     summarize on vertex label true/false
   * @param useEdgeLabels       summarize on edge label true/false
   */
  Summarization(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys,
    boolean useVertexLabels, boolean useEdgeLabels) {
    this.vertexGroupingKeys = checkNotNull(vertexGroupingKeys);
    this.edgeGroupingKeys   = checkNotNull(edgeGroupingKeys);
    this.useVertexLabels    = useVertexLabels;
    this.useEdgeLabels      = useEdgeLabels;
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
   * Build summarized edges by joining them with vertices and their group
   * representative.
   *
   * @param graph                     inout graph
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
      .with(new UpdateSourceId<E>(getEdgeGroupingKeys(), useEdgeLabels()))
      // join result with vertex-group-map on edge-target-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(2).equalTo(0)
      .with(new UpdateTargetId());

    return groupEdges(edges).reduceGroup(new BuildSummarizedEdge<>(
      getEdgeGroupingKeys(), useEdgeLabels(), config.getEdgeFactory()));
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
      groupedEdges = edges.groupBy(1, 2, 3, 4);
    } else if (useEdgeLabels()) {
      groupedEdges = edges.groupBy(1, 2, 3);
    } else if (useEdgeProperties()) {
      groupedEdges = edges.groupBy(1, 2, 4);
    } else {
      groupedEdges = edges.groupBy(1, 2);
    }
    return groupedEdges;
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graph
   * @return summarized output graph
   */
  protected abstract LogicalGraph<G, V, E> summarizeInternal(
    LogicalGraph<G, V, E> graph);
}
