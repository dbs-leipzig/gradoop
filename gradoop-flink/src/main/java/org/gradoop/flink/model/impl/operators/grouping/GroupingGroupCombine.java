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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertexBC;
import org.gradoop.flink.model.impl.operators.grouping.functions.CombineVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.TransposeVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.List;
import java.util.Optional;

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
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class GroupingGroupCombine<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends Grouping<G, V, E, LG, GC> {

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels             group on vertex label true/false
   * @param useEdgeLabels               group on edge label true/false
   * @param vertexLabelGroups           stores grouping properties for vertex labels
   * @param edgeLabelGroups             stores grouping properties for edge labels
   * @param retainVerticesWithoutGroups convert vertices that are not member of a group as is to
   *                                    supervertices
   */
  GroupingGroupCombine(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups,
    boolean retainVerticesWithoutGroups) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups,
      retainVerticesWithoutGroups);
  }

  @Override
  protected LG groupInternal(LG graph) {

    DataSet<V> vertices = graph.getVertices();

    Optional<LG> optionalRetainedVerticesSubgraph =
      isRetainingVerticesWithoutGroups() ? Optional.of(getRetainedVerticesSubgraph(graph)) :
        Optional.empty();

    if (optionalRetainedVerticesSubgraph.isPresent()) {
      vertices = getVerticesToGroup(vertices);
    }

    // map vertex to vertex group item
    DataSet<VertexGroupItem> verticesForGrouping = vertices.flatMap(
      new BuildVertexGroupItem<>(useVertexLabels(), getVertexLabelGroups()));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> combinedVertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function per combined partition
      .combineGroup(new CombineVertexGroupItems(useVertexLabels()));

    // filter super vertex tuples (1..n per partition/group)
    // group  super vertex tuples
    // create super vertex tuple (1 per group) + previous super vertex ids
    DataSet<Tuple2<VertexGroupItem, IdWithIdSet>> superVertexTuples =
      groupVertices(combinedVertexGroupItems.filter(new FilterSuperVertices()))
        .reduceGroup(new TransposeVertexGroupItems(useVertexLabels()));

    // build super vertices from super vertex tuples
    DataSet<V> superVertices = superVertexTuples
      .map(new Value0Of2<>())
      .map(new BuildSuperVertex<>(
        useVertexLabels(), graph.getFactory().getVertexFactory()));

    // extract mapping
    DataSet<IdWithIdSet> mapping = superVertexTuples
      .map(new Value1Of2<>());

    // filter non-candidates from combiner output
    // update their vertex representative according to the mapping
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = combinedVertexGroupItems
      .filter(new FilterRegularVertices())
      .map(new BuildVertexWithSuperVertexBC())
      .withBroadcastSet(mapping, BuildVertexWithSuperVertexBC.BC_MAPPING);

    DataSet<E> edgesToGroup = graph.getEdges();

    if (optionalRetainedVerticesSubgraph.isPresent()) {
      LG retainedVerticesSubgraph = optionalRetainedVerticesSubgraph.get();

      // To add support for grouped edges between retained vertices and supervertices,
      // vertices are their group representatives themselves
      vertexToRepresentativeMap =
        updateVertexRepresentativesWithUngroupedVertices(vertexToRepresentativeMap,
          retainedVerticesSubgraph);

      // don't execute grouping on edges between retained vertices
      // but execute on edges between retained vertices and grouped vertices
      //   graph.getEdges() - retainedVerticesSubgraph.getEdges()
      edgesToGroup = subtractRetainedEdgesFromDefaultGraph(graph, retainedVerticesSubgraph);
    }

    DataSet<E> superEdges =
      buildSuperEdges(graph.getFactory(), edgesToGroup, vertexToRepresentativeMap);

    if (optionalRetainedVerticesSubgraph.isPresent()) {

      LG retainedVerticesSubgraph = optionalRetainedVerticesSubgraph.get();
      superVertices = superVertices.union(retainedVerticesSubgraph.getVertices());
      superEdges = superEdges.union(retainedVerticesSubgraph.getEdges());
    }

    return graph.getFactory().fromDataSets(superVertices, superEdges);

  }

}
