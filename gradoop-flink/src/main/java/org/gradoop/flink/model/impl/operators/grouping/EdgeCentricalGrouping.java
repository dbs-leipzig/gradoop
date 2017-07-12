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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.BuildSuperEdges;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.BuildSuperVertices;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.BuildSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.BuildSuperVertexIdWithVertex;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.BuildVertexWithSuperVertexFromItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.CombineSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.FilterSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.PrepareSuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.PrepareSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.ReduceCombinedSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.ReduceSuperEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.UpdateSuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric.operators.SetInTupleKeySelector;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperVertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;

import java.util.List;

/**
 * An edge centrical grouping where the edges are grouped first and are represented by a
 * super edge. Additionally new super vertices are created based on the vertices of the original
 * edges. In this case it is possible that a super vertex represents different types of vertices.
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
   * @param vertexLabelGroups       stores grouping properties for vertex labels
   * @param edgeLabelGroups         stores grouping properties for edge labels
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
    boolean targetSpecificGrouping) {
    super(useVertexLabels, useEdgeLabels, vertexLabelGroups, edgeLabelGroups, groupingStrategy);

    this.sourceSpecificGrouping = sourceSpecificGrouping;
    this.targetSpecificGrouping = targetSpecificGrouping;
  }

  /**
   * Grouping implementation that uses group + groupReduce for building super
   * vertices and updating the original vertices.
   *
   * Algorithmic idea:
   *
   * 1) Map edges to a minimal representation, i.e. {@link SuperEdgeGroupItem}.
   * 2) Group edges on label and/or property and/or their source vertex and/or their target vertex
   * 3) Prepare minimal super vertex representations, i.e. {@link SuperVertexGroupItem} and
   *    group them based on the vertices they represent. If a super vertex represents multiple
   *    vertices create a new super vertex id, but if it represents only one vertex the keep the id.
   * 4) Cogroup the {@link SuperEdgeGroupItem} with the {@link SuperVertexGroupItem} based on the
   *    registered super edge id in both items and create the super edge with the corresponding
   *    super vertex ids as source/target.
   * 5) Filter those {@link SuperVertexGroupItem} where the vertex is its own super vertex and
   *    join them with the corresponding vertex from the input graph.
   * 6) Filter the {@link SuperVertexGroupItem} which represent multiple vertices.
   * 7) And assign the super vertex id to each of these vertex ids.
   * 8) Cogroup the {@link SuperVertexGroupItem}s with the represented vertices taken by a join
   *    of the result of 7) with the graphs vertices and aggregate the vertex properties and
   *    concatenate the label.
   * 9) Union the vertices which are their own super vertex with new created super vertices based
   *    on the {@link SuperVertexGroupItem}s.
   *
   * @param graph input graph
   * @return grouped output graph
   */
  @Override
  protected LogicalGraph groupReduce(LogicalGraph graph) {

    DataSet<SuperEdgeGroupItem> edgesForGrouping = graph.getEdges().rebalance()
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
      .groupBy(new SetInTupleKeySelector<SuperVertexGroupItem>(0))
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

    // keep only super vertices which represent multiple vertices
    superVertexGroupItems = superVertexGroupItems
      .filter(new FilterSuperVertexGroupItem(true));

    // get each single relevant vertex id for a super vertex id
    DataSet<VertexWithSuperVertex> vertexWithSuper = superVertexGroupItems
      // assign the vertex ids
      .flatMap(new BuildVertexWithSuperVertexFromItem());


    superVertexGroupItems = superVertexGroupItems
      // take all vertices by their id, which is stored in the super vertex group item, and
      // assigns them to the super vertex id
      .coGroup(vertexWithSuper
        .join(graph.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new BuildSuperVertexIdWithVertex()))
      .where(1).equalTo(0)
      // and aggregate the super vertex group item based on the vertex aggregations
      .with(new UpdateSuperVertexGroupItem(useVertexLabels()));

    DataSet<Vertex> superVertices = superVertexGroupItems
      // and create the vertex element
      .map(new BuildSuperVertices(useVertexLabels(), config.getVertexFactory()))
      // union with vertices which stay as they are
      .union(normalSuperVertices);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }

  @Override
  protected LogicalGraph groupCombine(LogicalGraph graph) {

    DataSet<SuperEdgeGroupItem> edgesForGrouping = graph.getEdges().rebalance()
      // map edge to edge group item
      .flatMap(new PrepareSuperEdgeGroupItem(useEdgeLabels(), getEdgeLabelGroups()));

    // group edges by label / properties / both
    // additionally: source specific / target specific / both
    DataSet<SuperEdgeGroupItem> combinedSuperEdgeGroupItems = groupSuperEdges(edgesForGrouping,
      sourceSpecificGrouping, targetSpecificGrouping)
      // apply aggregate function
      .combineGroup(new CombineSuperEdgeGroupItems(useEdgeLabels(), sourceSpecificGrouping,
        targetSpecificGrouping));

    DataSet<SuperEdgeGroupItem> superEdgeGroupItems = groupSuperEdges(combinedSuperEdgeGroupItems,
      sourceSpecificGrouping, targetSpecificGrouping)
      .reduceGroup(new ReduceCombinedSuperEdgeGroupItems(useEdgeLabels(), sourceSpecificGrouping,
       targetSpecificGrouping));


      // vertexIds - superVId - edgeId
    DataSet<SuperVertexGroupItem> superVertexGroupItems = superEdgeGroupItems
      // get all resulting (maybe concatenated) vertices
      // vertexIds - superedgeId
      .flatMap(new PrepareSuperVertexGroupItem(useVertexLabels(), getVertexLabelGroups()))
      // groups same super vertices (created from edge source and target ids)
      .groupBy(new SetInTupleKeySelector<SuperVertexGroupItem>(0))
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

    // keep only super vertices which represent multiple vertices
    superVertexGroupItems = superVertexGroupItems
      .filter(new FilterSuperVertexGroupItem(true));

    // get each single relevant vertex id for a super vertex id
    DataSet<VertexWithSuperVertex> vertexWithSuper = superVertexGroupItems
      // assign the vertex ids
      .flatMap(new BuildVertexWithSuperVertexFromItem());


    superVertexGroupItems = superVertexGroupItems
      // take all vertices by their id, which is stored in the super vertex group item, and
      // assigns them to the super vertex id
      .coGroup(vertexWithSuper
        .join(graph.getVertices())
        .where(0).equalTo(new Id<>())
        .with(new BuildSuperVertexIdWithVertex()))
      .where(1).equalTo(0)
      // and aggregate the super vertex group item based on the vertex aggregations
      .with(new UpdateSuperVertexGroupItem(useVertexLabels()));

    DataSet<Vertex> superVertices = superVertexGroupItems
      // and create the vertex element
      .map(new BuildSuperVertices(useVertexLabels(), config.getVertexFactory()))
      // union with vertices which stay as they are
      .union(normalSuperVertices);

    return LogicalGraph.fromDataSets(superVertices, superEdges, graph.getConfig());
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return EdgeCentricalGrouping.class.getName() + ":" + getGroupingStrategy();
  }
}
