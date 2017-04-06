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

package org.gradoop.flink.model.impl.operators.nest.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.CombineGraphBelongingInformation;
import org.gradoop.flink.model.impl.operators.nest.functions.DoQuadMatchTarget;
import org.gradoop.flink.model.impl.operators.nest.functions.DuplicateEdgeInformations;
import org.gradoop.flink.model.impl.operators.nest.functions.GetVerticesToBeNested;
import org.gradoop.flink.model.impl.operators.nest.functions.LeftSideIfRightNull;
import org.gradoop.flink.model.impl.operators.nest.functions.QuadEdgeDifference;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdgeSource;
import org.gradoop.flink.model.impl.operators.nest.functions.AsQuadsMatchingSource;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectEdgesPreliminary;
import org.gradoop.flink.model.impl.operators.nest.functions.MapGradoopIdAsVertex;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex0;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex4;
import org.gradoop.flink.model.impl.operators.nest.functions.HexMatch;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;
import org.gradoop.flink.model.impl.operators.nest.model.indices.IndexingAfterNesting;
import org.gradoop.flink.model.impl.operators.nest.model.indices.IndexingBeforeNesting;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.ops.BinaryOp;

/**
 * Establishing the edges using the operands
 */
public class DisjunctiveEdges extends BinaryOp<IndexingBeforeNesting, NestingIndex, IndexingAfterNesting> {

  /**
   * GraphId to be associated to the graph that is going to be returned by this operator
   */
  private final GradoopId newGraphId;

  /**
   * Constructor for specifying the to-be-returned graph's head
   * @param newGraphId                  the aforementioned id
   */
  public DisjunctiveEdges(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  @Override
  protected IndexingAfterNesting runWithArgAndLake(LogicalGraph dataLake,
    IndexingBeforeNesting nested,
    NestingIndex hypervertices) {

    DataSet<Hexaplet> hexas = nested.getPreviousComputation();
    DataSet<GradoopId> gh = nested.getGraphHeads();

    // The vertices appearing in a nested graph are the ones that induce the to-be-updated edges.
    DataSet<Hexaplet> verticesPromotingEdgeUpdate = hexas.filter(new GetVerticesToBeNested());

    DataSet<Tuple2<GradoopId, GradoopId>> gids = nested.getGraphHeadToEdge()
      .leftOuterJoin(hypervertices.getGraphHeadToEdge())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new LeftSideIfRightNull<>());

    // Edges to return and update are the ones that do not appear in the collection
    // TODO       JOIN COUNT: (2) -> NotInGraphBroadcast (a)
    DataSet<Hexaplet> edgesToUpdateOrReturn = dataLake.getEdges()
      // Each edge is associated to each possible graph
        .map(new AsQuadsMatchingSource())
        // (1) Now, we want to select the edge information only for the graphs in gU
        .joinWithTiny(gids)
        .where(new Hex0()).equalTo(new Value1Of2<>())
        .with(new CombineGraphBelongingInformation())
        .distinct(0)
        // (2) Mimicking the NotInGraphBroadcast
        .leftOuterJoin(hypervertices.getGraphHeadToEdge())
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
      .flatMap(new CollectEdges(newGraphId, true));

    return new IndexingAfterNesting(gh, nested.getGraphHeadToVertex(), edges, updatedEdges);
  }

  /**
   * Updates the ground truth by using the outcomes of the previous computations
   * @param dataLake  ground truth information
   * @param ian       indexed information
   * @return          updated ground truth information
   */
  public LogicalGraph updateFlatModel(LogicalGraph dataLake, IndexingAfterNesting ian) {
    DataSet<GradoopId> gh = ian.getGraphHeads();

    // Create new edges in the dataLake
    DataSet<Edge> newlyCreatedEdges = dataLake.getEdges()
      // Associate each edge to each new edge where he has generated from
      .coGroup(ian.getPreviousComputation())
      .where(new Id<>()).equalTo(new Hex0())
      .with(new DuplicateEdgeInformations());

    // Updates the data lake with a new model
    return LogicalGraph.fromDataSets(dataLake.getGraphHead(),
      dataLake.getVertices().union(gh.map(new MapGradoopIdAsVertex())),
      dataLake.getEdges().union(newlyCreatedEdges),
      dataLake.getConfig());
  }

  @Override
  public String getName() {
    return getClass().getName();
  }
}
