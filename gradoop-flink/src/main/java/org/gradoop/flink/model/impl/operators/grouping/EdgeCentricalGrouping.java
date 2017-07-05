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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.*;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.operators.SetInTupleKeySelector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;

/**
 *
 */
public class EdgeCentricalGrouping extends CentricalGrouping {

  /**
   * True, iff the source vertex shall be considered for grouping.
   */
  private boolean sourceSpecificGrouping;

  /**
   * True, iff the target vertex shall be considered for grouping.
   */
  private boolean targetSpecificGrouping;

  /**
   * Creates an edge grouping grouping operator instance.
   *
   * @param useVertexLabels         group on vertex label true/false
   * @param useEdgeLabels           group on edge label true/false
   * @param groupingStrategy        group by vertices or edges
   * @param sourceSpecificGrouping  set true to consider source on grouping
   * @param targetSpecificGrouping  set true to consider target on grouping
   */
  public EdgeCentricalGrouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups,
    GroupingStrategy groupingStrategy,
    boolean sourceSpecificGrouping,
    boolean targetSpecificGrouping
  ) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups, groupingStrategy);

    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {

    DataSet<SuperEdgeGroupItem> edgesForGrouping = graph.getEdges()
      // map edge to edge group item
      .flatMap(new PrepareSuperEdgeGroupItem(useEdgeLabels(), getEdgeLabelGroups()));

    // group edges by label / properties / both
    // additionally: source specific / target specific / both
    DataSet<SuperEdgeGroupItem> superEdgeGroupItems = groupSuperEdges(edgesForGrouping,
      sourceSpecificGrouping, targetSpecificGrouping)
      // apply aggregate function
      .reduceGroup(new ReduceSuperEdgeGroupItems(useEdgeLabels(), sourceSpecificGrouping,
        targetSpecificGrouping));

    // vertexIds - superVId - edgeId
    DataSet<SuperVertexGroupItem> superVertexGroupItems = superEdgeGroupItems
      // get all resulting (maybe concatenated) vertices
      // vertexIds - superedgeId
      .flatMap(new PrepareSuperVertexGroupItem(useVertexLabels(), getVertexLabelGroups()))
      // groups same super vertices (created from edge source and target ids)
      .groupBy(new SetInTupleKeySelector<SuperVertexGroupItem, GradoopId>(0))
      // assign supervertex id
      // vertexIds - superVId - edgeId - label - groupingVal - aggregatVal (last 3 are empty)
      .reduceGroup(new BuildSuperVertexGroupItem());

    // create super edges based on grouped edges and resulting super vertex ids
    DataSet<Edge> superEdges = superEdgeGroupItems
      .coGroup(superVertexGroupItems)
      // same super vertex id
      .where(0).equalTo(2)
      // build super edges
      .with(new BuildSuperEdges(useEdgeLabels(), config.getEdgeFactory()));

    // store vertices, where the vertex is its own super vertex, so the vertex stays itself
    DataSet<Vertex> normalSuperVertices = superVertexGroupItems
      // filter ids where vertex is its own super vertex
      .filter(new FilterSuperVertexGroupItem(false))
      // remove doubled entries where the vertex is part of different edges
      .distinct(1)
      // take vertex respectively to the filtered id
      .join(graph.getVertices())
      .where(1)
      .equalTo(new Id<>())
      .with(new RightSide<>());

    // get for a super vertex id each single relevant vertex id
    DataSet<VertexWithSuperVertex> vertexWithSuper = superVertexGroupItems
      // only real super vertices
      .filter(new FilterSuperVertexGroupItem(true))
      // assign the vertex ids
      .flatMap(new BuildVertexWithSuperVertexFromItem());

    superVertexGroupItems = superVertexGroupItems
      // take all vertices by their id, which is stored in the super vertex group item
      .coGroup(vertexWithSuper
        .join(graph.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new BuildSuperVertexIdWithVertex()))
      .where(1).equalTo(0)
      // and aggregate the super vertex group item based on the vertex aggregations
      .with(new UpdateSuperVertexGroupItem(useVertexLabels()));

    DataSet<Vertex> superVertices = superVertexGroupItems
      // take super vertex ids where vertex is not its own super vertex
      .filter(new FilterSuperVertexGroupItem(true))
      // and create the vertex element
      .map(new BuildSuperVertices(useVertexLabels(), config.getVertexFactory()))
      // union with vertices which stay as they are
      .union(normalSuperVertices);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
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