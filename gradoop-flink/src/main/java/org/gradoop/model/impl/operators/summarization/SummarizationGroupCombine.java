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
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.summarization.functions.BuildVertexGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.ReduceVertexGroupItem;
import org.gradoop.model.impl.operators.summarization.functions.FilterGroupRepresentatives;
import org.gradoop.model.impl.operators.summarization.functions.FilterSumVertexCandidates;
import org.gradoop.model.impl.operators.summarization.functions.BuildSummarizedVertex;
import org.gradoop.model.impl.operators.summarization.functions.BuildVertexWithRepresentative;
import org.gradoop.model.impl.operators.summarization.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.summarization.tuples.VertexWithRepresentative;

import java.util.List;

/**
 * Summarization implementation that does not require sorting of vertex groups.
 *
 * Note: This implementation uses the group combine operator in Flink. The
 * operator is performed in memory and throws exceptions when the data to
 * process exceed available memory.
 *
 * Algorithmic idea:
 *
 * 1) Group vertices on label and/or property
 * 2) Combine groups:
 *    - convert vertex data to smaller representation {@link VertexGroupItem}
 *    - create partition representative tuple (partition count, label/prop)
 * 3) Group combine output on label and/or property
 * 4) Reduce groups
 *    - create group representative from all partitions
 *    - update input tuples with group representative and collect them
 *    - create group representative tuple (group count, group label/prop)
 * 5) Filter group representative tuples from group reduce output
 *    - build summarized vertices
 * 6) Filter non group representative tuples from group reduce output
 *    - build {@link VertexWithRepresentative} tuples
 * 7) Join output from 6) with edges
 *    - replace source / target vertex id with vertex group representative
 * 8) Group edges on source/target vertex and value and/or property
 * 9) Build summarized edges
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class SummarizationGroupCombine<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Summarization<G, V, E> {
  /**
   * Creates summarization.
   *
   * @param vertexGroupingKeys  property keys to summarize vertices
   * @param edgeGroupingKeys    property keys to summarize edges
   * @param useVertexLabels     summarize on vertex label true/false
   * @param useEdgeLabels       summarize on edge label true/false
   */
  public SummarizationGroupCombine(
    List<String> vertexGroupingKeys, List<String> edgeGroupingKeys,
    boolean useVertexLabels, boolean useEdgeLabels) {
    super(vertexGroupingKeys, edgeGroupingKeys, useVertexLabels, useEdgeLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<G, V, E> summarizeInternal(
    LogicalGraph<G, V, E> graph) {

    // map vertex data to a smaller representation for grouping
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      .map(new BuildVertexGroupItem<V>(
        getVertexGroupingKeys(), useVertexLabels()));

    // group vertices by either label and/or both
    UnsortedGrouping<VertexGroupItem> groupedVertices =
      groupVertices(verticesForGrouping);

    // combine groups on partitions
    DataSet<VertexGroupItem> combinedGroupItems =
      groupedVertices.combineGroup(
        new ReduceVertexGroupItem(useVertexLabels()));

    // group partition output again
    UnsortedGrouping<VertexGroupItem> groupedGroupItems =
      groupVertices(combinedGroupItems);

    // group reduce groups
    GroupReduceOperator<VertexGroupItem, VertexGroupItem> reduceGroup =
      groupedGroupItems
        .reduceGroup(new ReduceVertexGroupItem(useVertexLabels()));

    // filter group representative tuples and build final vertices
    DataSet<V> summarizedVertices =
      reduceGroup.filter(new FilterSumVertexCandidates())
        .map(new BuildSummarizedVertex<>(getVertexGroupingKeys(),
          useVertexLabels(), config.getVertexFactory()));

    // filter vertex to representative tuples (used for edge join)
    DataSet<VertexWithRepresentative> vertexToRepresentativeMap =
      reduceGroup.filter(new FilterGroupRepresentatives())
        .map(new BuildVertexWithRepresentative());

    // build summarized edges
    DataSet<E> summarizedEdges = buildSummarizedEdges(
      graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(
      summarizedVertices, summarizedEdges, graph.getConfig());
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return SummarizationGroupCombine.class.getName();
  }
}
