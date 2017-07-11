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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.BuildVertexWithSuperVertexBC;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.CombineVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.vertexcentric.TransposeVertexGroupItems;

import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.tuples.vertexcentric.EdgeGroupItem;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.List;
import java.util.Objects;

/**
 * A vertex centrical grouping where the vertices are grouped first and are represented by a
 * super vertex. Additionally new super edges are created based on the edges of the
 * original vertices.
 */
public class VertexCentricalGrouping extends CentricalGrouping {

  /**
   * Creates grouping operator instance.
   *
   * @param useVertexLabels   group on vertex label true/false
   * @param useEdgeLabels     group on edge label true/false
   * @param vertexLabelGroups stores grouping properties for vertex labels
   * @param edgeLabelGroups   stores grouping properties for edge labels
   * @param groupingStrategy  grouping strategy
   */
  public VertexCentricalGrouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups,
    GroupingStrategy groupingStrategy) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups, groupingStrategy);

    Objects.requireNonNull(getGroupingStrategy(), "Missing vertex grouping strategy.");
  }

  /**
   * Grouping implementation that uses group + groupReduce for building super
   * vertices and updating the original vertices.
   *
   * Algorithmic idea:
   *
   * 1) Map vertices to a minimal representation, i.e. {@link VertexGroupItem}.
   * 2) Group vertices on label and/or property.
   * 3) Create a super vertex id for each group and collect a non-candidate
   *    {@link VertexGroupItem} for each group element and one additional
   *    super vertex tuple that holds the group aggregate.
   * 4) Filter output of 3)
   *    a) non-candidate tuples are mapped to {@link VertexWithSuperVertex}
   *    b) super vertex tuples are used to build final super vertices
   * 5) Map edges to a minimal representation, i.e. {@link EdgeGroupItem}
   * 6) Join edges with output of 4a) and replace source/target id with super
   *    vertex id.
   * 7) Updated edges are grouped by source and target id and optionally by label
   *    and/or edge property.
   * 8) Group combine on the workers and compute aggregate.
   * 9) Group reduce globally and create final super edges.
   *
   * @param graph input graph
   * @return grouped output graph
   */
  protected LogicalGraph groupReduce(LogicalGraph graph) {
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .flatMap(new BuildVertexGroupItem(useVertexLabels(), getVertexLabelGroups()));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> vertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceVertexGroupItems(useVertexLabels()));

    DataSet<Vertex> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterSuperVertices())
      // build super vertices
      .map(new BuildSuperVertex(useVertexLabels(), config.getVertexFactory()));

    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = vertexGroupItems
      // filter group element tuples
      .filter(new FilterRegularVertices())
      // build vertex to group representative tuple
      .map(new BuildVertexWithSuperVertex());

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }

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
   * @param graph input graph
   * @return grouped output graph
   */
  protected LogicalGraph groupCombine(LogicalGraph graph) {
    // map vertex to vertex group item
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      .flatMap(new BuildVertexGroupItem(useVertexLabels(), getVertexLabelGroups()));

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
    DataSet<Vertex> superVertices = superVertexTuples
      .map(new Value0Of2<>())
      .map(new BuildSuperVertex(useVertexLabels(), config.getVertexFactory()));

    // extract mapping
    DataSet<IdWithIdSet> mapping = superVertexTuples
      .map(new Value1Of2<>());

    // filter non-candidates from combiner output
    // update their vertex representative according to the mapping
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = combinedVertexGroupItems
      .filter(new FilterRegularVertices())
      .map(new BuildVertexWithSuperVertexBC())
      .withBroadcastSet(mapping, BuildVertexWithSuperVertexBC.BC_MAPPING);

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return VertexCentricalGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}
