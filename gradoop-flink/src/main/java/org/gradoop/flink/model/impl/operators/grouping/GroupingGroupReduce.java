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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions
  .BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions
  .BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions
  .BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;

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
 */
public class GroupingGroupReduce extends Grouping {
  /**
   * Creates grouping operator instance.
   *
   * @param vertexGroupingKeys  property key to group vertices
   * @param useVertexLabels     group on vertex label true/false
   * @param vertexAggregators   aggregate functions for grouped vertices
   * @param edgeGroupingKeys    property key to group edges
   * @param useEdgeLabels       group on edge label true/false
   * @param edgeAggregators     aggregate functions for grouped edges
   */
  GroupingGroupReduce(
    List<String> vertexGroupingKeys,
    boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators,
    List<String> edgeGroupingKeys,
    boolean useEdgeLabels,
    List<PropertyValueAggregator> edgeAggregators) {
    super(
      vertexGroupingKeys, useVertexLabels, vertexAggregators,
      edgeGroupingKeys, useEdgeLabels, edgeAggregators);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph groupInternal(LogicalGraph graph) {

    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .map(new BuildVertexGroupItem(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators()));

    DataSet<VertexGroupItem> vertexGroupItems =
      // group vertices by label / properties / both
      groupVertices(verticesForGrouping)
        // apply aggregate function
        .reduceGroup(new ReduceVertexGroupItems(
          useVertexLabels(), getVertexAggregators()));

    DataSet<Vertex> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterSuperVertices())
      // build super vertices
      .map(new BuildSuperVertex(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregators(), config.getVertexFactory()));

    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap =
      vertexGroupItems
        // filter group element tuples
        .filter(new FilterRegularVertices())
        // build vertex to group representative tuple
        .map(new BuildVertexWithSuperVertex());

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph,
      vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(superVertices, superEdges,
      graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return GroupingGroupReduce.class.getName();
  }
}
