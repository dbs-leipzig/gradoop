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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.AsQuadsMatchingSource;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.GetVerticesToBeNested;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex4;
import org.gradoop.flink.model.impl.operators.nest.functions.QuadEdgeDifference;
import org.gradoop.flink.model.impl.operators.nest.functions.Identity;
import org.gradoop.flink.model.impl.operators.nest.functions.LeftSideIfRightNull;
import org.gradoop.flink.model.impl.operators.nest.functions.ToTuple2WithF0;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex0;
import org.gradoop.flink.model.impl.operators.nest.functions.DuplicateEdgeInformations;
import org.gradoop.flink.model.impl.operators.nest.functions.CombineGraphBelongingInformation;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;
import org.gradoop.flink.model.impl.operators.nest.functions.HexMatch;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.ConstantZero;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdgesPreliminary;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdgeSource;
import org.gradoop.flink.model.impl.operators.nest.functions.DoQuadMatchTarget;

/**
 * Defines the nested model where the operations are actually carried out
 */
public class NestedModel {

  /**
   * Flat representation for all the other graphs
   */
  private LogicalGraph flattenedGraph;

  /**
   * Nested representation induced by the nesting functions
   */
  private NestingIndex nestedRepresentation;

  /**
   * Nesting result of the previous nest operation
   */
  private NestingResult previousResult;

  /**
   * Implements the nested model using the most generic components
   * @param flattenedGraph        Flattened graph representing all the possible data values
   * @param nestedRepresentation  Nesting structures over the flattened graph
   */
  public NestedModel(LogicalGraph flattenedGraph, NestingIndex nestedRepresentation) {
    this.flattenedGraph = flattenedGraph;
    this.nestedRepresentation = nestedRepresentation;
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
  public NestingIndex getNestedRepresentation() {
    return nestedRepresentation;
  }

  /**
   * Generating a nested model from the initial graph operands
   * @param graph           Graph element that is going to undergo the nesting
   * @param collection      Collection defining the nesting structure
   * @param graphIndex      Graph index previously evaluated
   * @param collectionIndex Collection index previously evaluated
   * @return   The associated nested model
   */
  public static NestedModel generateNestedModelFromOperands(LogicalGraph graph,
    GraphCollection collection, NestingIndex graphIndex, NestingIndex collectionIndex) {

    DataSet<GraphHead> heads = graph.getGraphHead()
      .union(collection.getGraphHeads())
      .distinct(new Id<>());

    DataSet<Vertex> nestedVertices = heads
      .map(new GraphHeadToVertex());

    DataSet<Vertex> vertices = graph.getVertices()
      .union(collection.getVertices())
      .union(nestedVertices)
      .distinct(new Id<>());

    DataSet<Edge> edges = graph.getEdges()
      .union(collection.getEdges())
      .distinct(new Id<>());

    // Getting the model for defining the associated model
    LogicalGraph flattenedGraph = LogicalGraph.fromDataSets
      (heads, vertices, edges, graph.getConfig());

    NestingIndex nestedRepresentation = NestingBase.mergeIndices(graphIndex, collectionIndex);

    return new NestedModel(flattenedGraph, nestedRepresentation);
  }

  /**
   * Generating a nested model from the initial graph operands, when graph indices are not already
   * evaluated
   * @param graph           Graph element that is going to undergo the nesting
   * @param collection      Collection defining the nesting structure
   * @return    The associated nested model
   */
  public static NestedModel generateNestedModelFromOperands(LogicalGraph graph,
    GraphCollection collection) {

    NestingIndex graphIndex = NestingBase.createIndex(graph);
    NestingIndex collectionIndex = NestingBase.createIndex(collection);
    return generateNestedModelFromOperands(graph, collection, graphIndex, collectionIndex);
  }

  /**
   * Returns…
   * @return the previous nesting operation result
   */
  public NestingResult getPreviousResult() {
    return previousResult;
  }

  /**
   * Implements the nesting operation for the nested model. The returned graph is just a view
   * on top of the flattened graph
   *
   * @param graphIndex        index for the search graph
   * @param collectionIndex   index for the graph collection
   * @param nestedGraphId     id to be associated to the new graph in the EPGM model
   * @return                  The updated results associated to the new graph
   */
  public NestingResult nesting(NestingIndex graphIndex, NestingIndex collectionIndex,
    GradoopId nestedGraphId) {
    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<GradoopId> heads = graphIndex.getGraphHeads();

    DataSet<Tuple2<GradoopId, GradoopId>> newStackElement = heads
      .map(new ToTuple2WithF0(nestedGraphId));

    // Creates a new element to the stack
    nestedRepresentation.updateStackWith(newStackElement);

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> nestedResult = graphIndex.getGraphHeadToVertex()
      .leftOuterJoin(collectionIndex.getGraphHeadToVertex())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      // If the vertex does not appear in the graph collection, the f2 element is null.
      // These vertices are the ones to be returned as vertices alongside with the new
      // graph heads
      .with(new AssociateAndMark());

    // Vertices to be returend within the NestedIndexing
    DataSet<GradoopId> tmpVert = nestedResult
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices());

    DataSet<Tuple2<GradoopId, GradoopId>> vertices = heads
      .crossWithHuge(tmpVert);

    DataSet<GradoopId> tmpEdges = graphIndex
      .getGraphHeadToEdge()
      .map(new Value1Of2<>());

    DataSet<Tuple2<GradoopId, GradoopId>> edges = heads
      .crossWithHuge(tmpEdges);

    previousResult = new NestingResult(heads, vertices, edges, nestedResult, newStackElement);

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
      .where(new Value1Of2<>()).equalTo(new Identity<>())
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
    DataSet<Hexaplet> verticesPromotingEdgeUpdate = hexas.filter(new GetVerticesToBeNested());

    DataSet<Tuple2<GradoopId, GradoopId>> gids = nested.getGraphHeadToEdge()
      .leftOuterJoin(collection.getGraphHeadToEdge())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new LeftSideIfRightNull<>());

    // Edges to return and update are the ones that do not appear in the collection
    // TODO       JOIN COUNT: (2) -> NotInGraphBroadcast (a)
    DataSet<Hexaplet> edgesToUpdateOrReturn = flattenedGraph.getEdges()
      // Each edge is associated to each possible graph
      .map(new AsQuadsMatchingSource())
      // (1) Now, we want to select the edge information only for the graphs in gU
      .joinWithTiny(gids)
      .where(new Hex0()).equalTo(new Value1Of2<>())
      .with(new CombineGraphBelongingInformation())
      .distinct(0)
      // (2) Mimicking the NotInGraphBroadcast
      .leftOuterJoin(collection.getGraphHeadToEdge())
      .where(new Hex4()).equalTo(new Value1Of2<>())
      .with(new QuadEdgeDifference());

    // I have to only add the edges that are matched and updated
    // TODO       JOIN COUNT: (2)
    DataSet<Hexaplet> updatedEdges = edgesToUpdateOrReturn
      // Update the vertices' source
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new HexMatch()).equalTo(new HexMatch())
      .with(new UpdateEdgeSource(true))
      // Now start the match with the targets
      .map(new DoQuadMatchTarget())
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new HexMatch()).equalTo(new HexMatch())
      .with(new UpdateEdgeSource(false));

    // Edges to be set within the NestedIndexing
    DataSet<Tuple2<GradoopId, GradoopId>> edges = updatedEdges
      .map(new CollectEdgesPreliminary())
      .joinWithTiny(inferEPGMGraphHeadIdFromNestingResult(nested))
      .where(new ConstantZero<>()).equalTo(new ConstantZero<>())
      .with(new CollectEdges(true));

    nestedRepresentation.incrementallyUpdateEdges(edges);
    previousResult = new NestingResult(gh, nested.getGraphHeadToVertex(), edges, updatedEdges,
      nested.getGraphStack());

    DataSet<Edge> newlyCreatedEdges = flattenedGraph.getEdges()
      // Associate each edge to each new edge where he has generated from
      .coGroup(previousResult.getPreviousComputation())
      .where(new Id<>()).equalTo(new Hex0())
      .with(new DuplicateEdgeInformations());

    // Updates the data lake with a new model
    flattenedGraph = LogicalGraph.fromDataSets(flattenedGraph.getGraphHead(),
      flattenedGraph.getVertices(),
      flattenedGraph.getEdges().union(newlyCreatedEdges),
      flattenedGraph.getConfig());

    return previousResult;
  }

}
