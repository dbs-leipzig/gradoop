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
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.summarization.functions.BuildVertexGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.FilterNonCandidates;
import org.gradoop.model.impl.operators.summarization.functions.FilterCandidates;
import org.gradoop.model.impl.operators.summarization.functions.BuildSummarizedVertex;
import org.gradoop.model.impl.operators.summarization.functions.BuildVertexWithRepresentative;
import org.gradoop.model.impl.operators.summarization.functions.ReduceVertexGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;

import java.util.List;

/**
 * Summarization implementation that does not require sorting of vertex groups.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a minimal representation {@link VertexGroupItem}
 * 2) Group vertices on label and/or property.
 * 3) Reduce group and collect one {@link VertexGroupItem} for each group
 *    element and one additional {@link VertexGroupItem} for the group that
 *    holds the group count.
 * 4) Filter output of 3)
 *    a)  tuples with group count == 0 are mapped to
 *        {@link VertexWithRepresentative}
 *    b) tuples with group count > 0 are used to build final summarized vertices
 * 5) Output of 4a) is joined with edges
 * 6) Edge source and target vertex ids are replaced by group representative.
 * 7) Edges are grouped by source and target id and optionally by label
 *    and/or edge property.
 * 8) Group reduce edges and create final summarized edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class SummarizationGroupMap<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Summarization<G, V, E> {
  /**
   * Creates summarization.
   *
   * @param vertexGroupingKeys      property key to summarize vertices
   * @param useVertexLabels         summarize on vertex label true/false
   * @param vertexAggregateFunction aggregate function for summarized vertices
   * @param edgeGroupingKeys        property key to summarize edges
   * @param useEdgeLabels           summarize on edge label true/false
   * @param edgeAggregateFunction   aggregate function for summarized edges
   */
  SummarizationGroupMap(
    List<String> vertexGroupingKeys,
    boolean useVertexLabels,
    PropertyValueAggregator vertexAggregateFunction,
    List<String> edgeGroupingKeys,
    boolean useEdgeLabels,
    PropertyValueAggregator edgeAggregateFunction) {
    super(
      vertexGroupingKeys, useVertexLabels, vertexAggregateFunction,
      edgeGroupingKeys, useEdgeLabels, edgeAggregateFunction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<G, V, E> summarizeInternal(
    LogicalGraph<G, V, E> graph) {

    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .map(new BuildVertexGroupItem<V>(
        getVertexGroupingKeys(), useVertexLabels(), getVertexAggregator()));

    DataSet<VertexGroupItem> vertexGroupItems =
      // group vertices by label / properties / both
      groupVertices(verticesForGrouping)
        // apply aggregate function
        .reduceGroup(
          new ReduceVertexGroupItem(useVertexLabels(), getVertexAggregator()));

    DataSet<V> summarizedVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterCandidates())
      // build summarized vertex
      .map(new BuildSummarizedVertex<>(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregator(), config.getVertexFactory()));

    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      vertexGroupItems
        // filter group element tuples
        .filter(new FilterNonCandidates())
        // build vertex to group representative tuple
        .map(new BuildVertexWithRepresentative());

    // build summarized edges
    DataSet<E> summarizedEdges =
      buildSummarizedEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(
      summarizedVertices, summarizedEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupMap.class.getName();
  }
}
