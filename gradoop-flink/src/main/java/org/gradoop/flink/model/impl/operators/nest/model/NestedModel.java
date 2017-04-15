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

package org.gradoop.flink.model.impl.operators.nest.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.Self;
import org.gradoop.flink.model.impl.operators.nest.functions.AsEdgesMatchingSource;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdgesPreliminary;
import org.gradoop.flink.model.impl.operators.nest.functions.CombineGraphBelongingInformation;
import org.gradoop.flink.model.impl.operators.nest.functions.ConstantZero;
import org.gradoop.flink.model.impl.operators.nest.functions.DoHexMatchTarget;
import org.gradoop.flink.model.impl.operators.nest.functions.DuplicateEdgeInformations;
import org.gradoop.flink.model.impl.operators.nest.functions.GetVerticesToBeNested;
import org.gradoop.flink.model.impl.operators.nest.functions.HexapletEdgeDifference;
import org.gradoop.flink.model.impl.operators.nest.functions.LeftSideIfRightNull;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdgeSource;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Defines the nested model where the operations are actually carried out.
 *
 * All the fields of this class are passed to subsequent operators implementing
 * {@code WithNestedResult}
 */
public class NestedModel {

  /**
   * Flat representation for all the other graphs
   */
  private LogicalGraph flattenedGraph;

  /**
   * Nested representation induced by the nesting functions.
   */
  private NestingIndex nestingIndex;

  /**
   * Nesting result of the previous nest operation
   */
  private NestingResult previousResult;

  /**
   * Implements the nested model using the most generic components
   * @param flattenedGraph        Flattened graph representing all the possible data values
   * @param nestingIndex  Nesting structures over the flattened graph
   */
  public NestedModel(LogicalGraph flattenedGraph, NestingIndex nestingIndex) {
    this.flattenedGraph = flattenedGraph;
    this.nestingIndex = nestingIndex;
  }

  /**
   * Returns…
   * @return  the flattened representation for the graph
   */
  public LogicalGraph getFlattenedGraph() {
    return flattenedGraph;
  }

  /**
   * Returns…
   * @return  the indexing structure inducing the nesting
   */
  public NestingIndex getNestingIndex() {
    return nestingIndex;
  }

  /**
   * Returns…
   * @return the previous nesting operation result
   */
  public NestingResult getPreviousResult() {
    return previousResult;
  }

  /**
   * Determines the actual graph id in the EPGM model for the graph, here expressed as a
   * computational result
   * @param nestedGraph   Nested graph representation
   * @return              GradoopId on the EPGM model
   */
  public static DataSet<GradoopId> inferEPGMGraphHeadIdFromNestingResult(NestingResult
    nestedGraph) {
    return nestedGraph.getGraphStack()
      .joinWithTiny(nestedGraph.getGraphHeads())
      .where(1).equalTo(new Self<>())
      .with(new LeftSide<>())
      .map(new Value0Of2<>());
  }

  /**
   * Implements the disjunctive semantics fot the nested model. This is a partially materialized
   * view, where only the components pertaining to the edges are materialized in the flattened graph
   *
   * @param nested          Nested graph
   * @param collection      Collection over which evaluate the semantics
   * @return                The updated indices for the resulting nested graph
   */
  public NestingResult disjunctiveSemantics(NestingResult nested, NestingIndex collection) {

    DataSet<Hexaplet> hexas = nested.getPreviousComputation();
    DataSet<GradoopId> gh = nested.getGraphHeads();

    // The vertices appearing in a nested graph are the ones that induce the to-be-updated edges.
    DataSet<Hexaplet> verticesToSummarize = hexas.filter(new GetVerticesToBeNested());

    DataSet<Tuple2<GradoopId, GradoopId>> nestedGraphEdgeMap = nested.getGraphEdgeMap()
      .leftOuterJoin(collection.getGraphEdgeMap())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new LeftSideIfRightNull<>());

    // Edges to return and update are the ones that do not appear in the collection
    // TODO       JOIN COUNT: (2) -> NotInGraphBroadcast (a)
    DataSet<Hexaplet> edgesToUpdateOrReturn = flattenedGraph.getEdges()
      // Each edge is associated to each possible graph
      .map(new AsEdgesMatchingSource())
      // (1) Now, we want to select the edge information only for the graphs in the graph collection
      .coGroup(nestedGraphEdgeMap)
      .where(0).equalTo(1)
      .with(new CombineGraphBelongingInformation())
      //.distinct(0)
      // (2) Mimicking the NotInGraphBroadcast
      .leftOuterJoin(collection.getGraphEdgeMap())
      .where(4).equalTo(1)
      .with(new HexapletEdgeDifference())
    // I have to only add the edges that are matched and updated
    // TODO       JOIN COUNT: (2)
      // Update the vertices' source
      .leftOuterJoin(verticesToSummarize)
      .where(2).equalTo(2)
      .with(new UpdateEdgeSource(true))
      // Now start the match with the targets
      .map(new DoHexMatchTarget())
      .leftOuterJoin(verticesToSummarize)
      .where(2).equalTo(2)
      .with(new UpdateEdgeSource(false));

    // Edges to be set within the NestedIndexing
    DataSet<Tuple2<GradoopId, GradoopId>> edges = edgesToUpdateOrReturn
      .map(new CollectEdgesPreliminary())
      // I use a join creating a “very big key” because there are no FlatFunctions for Crosses
      .joinWithTiny(inferEPGMGraphHeadIdFromNestingResult(nested))
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new CollectEdges(true));

    nestingIndex.incrementallyUpdateEdges(edges);
    previousResult = new NestingResult(gh, nested.getGraphVertexMap(), edges, edgesToUpdateOrReturn,
      nested.getGraphStack());

    DataSet<Edge> newlyCreatedEdges = flattenedGraph.getEdges()
      // Associate each edge to each new edge where he has generated from
      .coGroup(previousResult.getPreviousComputation())
      .where(new Id<>()).equalTo(0)
      .with(new DuplicateEdgeInformations());

    // Updates the data lake with a new model
    flattenedGraph = LogicalGraph.fromDataSets(flattenedGraph.getGraphHead(),
      flattenedGraph.getVertices(),
      flattenedGraph.getEdges().union(newlyCreatedEdges),
      flattenedGraph.getConfig());

    return previousResult;
  }

  public void setPreviousResult(NestingResult previousResult) {
    this.previousResult = previousResult;
  }
}
