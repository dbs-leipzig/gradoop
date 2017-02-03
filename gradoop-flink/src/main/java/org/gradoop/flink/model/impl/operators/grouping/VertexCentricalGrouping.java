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
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertexBC;
import org.gradoop.flink.model.impl.operators.grouping.functions.CombineVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterRegularVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.TransposeVertexGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.model.impl.tuples.IdWithIdSet;

import java.util.List;
import java.util.Objects;

/**
 *
 */
public class VertexCentricalGrouping extends CentricalGrouping {


  public VertexCentricalGrouping(List<String> vertexGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> vertexAggregators, List<String> edgeGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> edgeAggregators,
    GroupingStrategy groupingStrategy) {
    super(vertexGroupingKeys, useVertexLabels, vertexAggregators, edgeGroupingKeys,
      useEdgeLabels, edgeAggregators, groupingStrategy);

    Objects.requireNonNull(getVertexGroupingKeys(), "missing vertex grouping key(s)");
    Objects.requireNonNull(getGroupingStrategy(), "missing vertex grouping strategy");
  }

  protected LogicalGraph groupReduce(LogicalGraph graph) {
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      // map vertex to vertex group item
      .map(new BuildVertexGroupItem(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators()));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> vertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceVertexGroupItems(useVertexLabels(), getVertexAggregators()));

    DataSet<Vertex> superVertices = vertexGroupItems
      // filter group representative tuples
      .filter(new FilterSuperVertices())
      // build super vertices
      .map(new BuildSuperVertex(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregators(), config.getVertexFactory()));

    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap = vertexGroupItems
      // filter group element tuples
      .filter(new FilterRegularVertices())
      // build vertex to group representative tuple
      .map(new BuildVertexWithSuperVertex());

    // build super edges
    DataSet<Edge> superEdges = buildSuperEdges(graph, vertexToRepresentativeMap);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }

  protected LogicalGraph groupCombine(LogicalGraph graph) {
    // map vertex to vertex group item
    DataSet<VertexGroupItem> verticesForGrouping = graph.getVertices()
      .map(new BuildVertexGroupItem(getVertexGroupingKeys(),
        useVertexLabels(), getVertexAggregators()));

    // group vertices by label / properties / both
    DataSet<VertexGroupItem> combinedVertexGroupItems = groupVertices(verticesForGrouping)
      // apply aggregate function per combined partition
      .combineGroup(new CombineVertexGroupItems(useVertexLabels(), getVertexAggregators()));

    // filter super vertex tuples (1..n per partition/group)
    // group  super vertex tuples
    // create super vertex tuple (1 per group) + previous super vertex ids
    DataSet<Tuple2<VertexGroupItem, IdWithIdSet>> superVertexTuples =
      groupVertices(combinedVertexGroupItems.filter(new FilterSuperVertices()))
        .reduceGroup(new TransposeVertexGroupItems(useVertexLabels(), getVertexAggregators()));

    // build super vertices from super vertex tuples
    DataSet<Vertex> superVertices = superVertexTuples
      .map(new Value0Of2<>())
      .map(new BuildSuperVertex(getVertexGroupingKeys(), useVertexLabels(),
        getVertexAggregators(), config.getVertexFactory()));

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
