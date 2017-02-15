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
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildSuperEdges;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildVertexWithSuperVertexAndEdge;
import org.gradoop.flink.model.impl.operators.grouping.functions.FilterSuperEdges;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;



import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeWithSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertexAndEdge;

import java.util.List;

/**
 *
 */
public class EdgeCentricalGrouping extends CentricalGrouping {

  private boolean sourceSpecificGrouping;

  private boolean targetSpecificGrouping;

  public EdgeCentricalGrouping(List<String> primaryGroupingKeys, boolean useVertexLabels,
    List<PropertyValueAggregator> primaryAggregators, List<String> secondaryGroupingKeys,
    boolean useEdgeLabels, List<PropertyValueAggregator> secondaryAggregators,
    GroupingStrategy groupingStrategy, Boolean sourceSpecificGrouping,
    Boolean targetSpecificGrouping) {
    super(primaryGroupingKeys, useVertexLabels, primaryAggregators, secondaryGroupingKeys,
      useEdgeLabels, secondaryAggregators, groupingStrategy);
    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {

    DataSet<EdgeWithSuperEdgeGroupItem> edgesForGrouping = graph.getEdges()
      // map edg to edge group item
      .map(new BuildEdgeWithSuperEdgeGroupItem(getEdgeGroupingKeys(), useEdgeLabels(),
        getEdgeAggregators()));

    //group vertices by label / properties / both
    // additionally: source specific / target specific / both
    DataSet<SuperEdgeGroupItem> superEdgeGroupItems = groupSuperEdges(edgesForGrouping,
      sourceSpecificGrouping, targetSpecificGrouping)
      //apply aggregate function
      .reduceGroup(new ReduceSuperEdgeGroupItems(useEdgeLabels(), getEdgeAggregators(),
        sourceSpecificGrouping, targetSpecificGrouping));

    DataSet<VertexWithSuperVertexAndEdge> vertexWithSuper = superEdgeGroupItems
      .flatMap(new BuildVertexWithSuperVertexAndEdge())
      .distinct();

    try {
      edgesForGrouping.print();
      superEdgeGroupItems.print();
//      vertexWithSuper.print();
    } catch (Exception e) {
      e.printStackTrace();
    }


    //TODO use vertexWithSuper to set the source and the target for each edge
    DataSet<Edge> superEdges = superEdgeGroupItems
      .coGroup(
        vertexWithSuper
          //.distinct(1))
      )
      .where(0)
      .equalTo(1)
      // build super edges
      .with(new BuildSuperEdges(getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators(),
        config.getEdgeFactory()));

    try {
      superEdges.print();
    } catch (Exception e) {
      e.printStackTrace();
    }


    return null;
  }

  @Override
  protected LogicalGraph groupCombine(LogicalGraph graph) {
    return null;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return EdgeCentricalGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}
