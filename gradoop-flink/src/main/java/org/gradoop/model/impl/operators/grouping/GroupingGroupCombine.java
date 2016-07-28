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

package org.gradoop.model.impl.operators.grouping;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.tuple.Value0Of2;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.model.impl.operators.grouping.functions.BuildVertexWithSuperVertexBC;
import org.gradoop.model.impl.operators.grouping.functions.CombineVertexGroupItems;
import org.gradoop.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.model.impl.operators.grouping.functions.TransposeVertexGroupItems;
import org.gradoop.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.model.impl.tuples.IdWithIdSet;

import java.util.List;

/**
 * Grouping implementation that uses group + groupCombine + groupReduce for
 * building super vertices and updating the original vertices.
 *
 * Algorithmic idea:
 *
 * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
 * 2) Group vertices on label and/or property
 * 3) Use groupCombine to process the grouped partitions. Creates a super vertex
 *    tuple for each group partition, including the local aggregates.
 *    Update each vertex tuple with their super vertex id and forward them.
 * 4) Filter output of 3)
 *    a) super vertex tuples are filtered, grouped and merged via groupReduce to
 *       create a final super vertex representing the group. An additional
 *       mapping from the final super vertex id to the super vertex ids of the
 *       original partitions is also created.
 *    b) non-candidate tuples are mapped to {@link VertexWithSuperVertex} using
 *       the broadcasted mapping output of 4a)
 * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
 * 6) Join edges with output of 4b) and replace source/target id with super
 *    vertex id.
 * 7) Updated edges are grouped by source and target id and optionally by label
 *    and/or edge property.
 * 8) Group combine on the workers and compute aggregate.
 * 9) Group reduce globally and create final super edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GroupingGroupCombine<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Grouping<G, V, E> {

  /**
   * Creates grouping operator instance.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param useVertexLabels    group on vertex label true/false
   * @param vertexAggregators  aggregate functions for grouped vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @param useEdgeLabels      group on edge label true/false
   * @param edgeAggregators    aggregate functions for grouped edges
   */
  GroupingGroupCombine(List<String> vertexGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators,
    List<String> edgeGroupingKeys, boolean useEdgeLabels,
    List<PropertyValueAggregator> edgeAggregators) {
    super(vertexGroupingKeys, useVertexLabels, vertexAggregators,
      edgeGroupingKeys, useEdgeLabels, edgeAggregators);
  }


  @Override
  protected LogicalGraph<G, V, E> groupInternal(LogicalGraph<G, V, E> graph) {
    // map vertex to vertex group item
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      .map(new BuildVertexGroupItem<V>(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregators()));

    DataSet<VertexGroupItem> combinedVertexGroupItems =
      // group vertices by label / properties / both
      groupVertices(verticesForGrouping)
        // apply aggregate function per combined partition
        .combineGroup(new CombineVertexGroupItems(
          useVertexLabels(), getVertexAggregators()));

    // filter super vertex tuples (1..n per partition/group)
    // group  super vertex tuples
    // create super vertex tuple (1 per group) + previous super vertex ids
    DataSet<Tuple2<VertexGroupItem, IdWithIdSet>>
      superVertexTuples = groupVertices(
        combinedVertexGroupItems.filter(new FilterSuperVertices()))
        .reduceGroup(new TransposeVertexGroupItems(useVertexLabels(),
          getVertexAggregators()));

    // build super vertices from super vertex tuples
    DataSet<V> superVertices = superVertexTuples
      .map(new Value0Of2<VertexGroupItem, IdWithIdSet>())
      .map(new BuildSuperVertex<>(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators(), config.getVertexFactory()));

    // extract mapping
    DataSet<IdWithIdSet> mapping = superVertexTuples
      .map(new Value1Of2<VertexGroupItem, IdWithIdSet>());

    // filter non-candidates from combiner output
    // update their vertex representative according to the mapping
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap =
      combinedVertexGroupItems
        .filter(new FilterRegularVertices())
        .map(new BuildVertexWithSuperVertexBC())
        .withBroadcastSet(mapping, BuildVertexWithSuperVertexBC.BC_MAPPING);

    // build super edges
    DataSet<E> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(
      superVertices, superEdges, graph.getConfig());
  }

  @Override
  public String getName() {
    return GroupingGroupCombine.class.getName();
  }
}
