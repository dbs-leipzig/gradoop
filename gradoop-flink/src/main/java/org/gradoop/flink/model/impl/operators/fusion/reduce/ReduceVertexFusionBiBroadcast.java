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

package org.gradoop.flink.model.impl.operators.fusion.reduce;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.GraphGraphGraphCollectionToGraph;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InBiGraphBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions
  .CoGroupAssociateOldVerticesWithNewIds;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.CoGroupGraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.FlatJoinSourceEdgeReference;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.LeftElementId;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions
  .MapFunctionAddGraphElementToGraph2;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapVertexToPairWithGraphId;
import org.gradoop.flink.model.impl.operators.fusion.reduce.functions.MapVerticesAsTuplesWithNullId;

import static org.gradoop.flink.model.impl.functions.graphcontainment
  .BiGraphContainmentFilterBroadcast.GRAPH_LEFT;
import static org.gradoop.flink.model.impl.functions.graphcontainment
  .BiGraphContainmentFilterBroadcast.GRAPH_RIGHT;

/**
 * Given two graph operands and a graph collection of (solved)
 * patterns, summarize the union of the two operands by
 * replacing each of the pattern with a hypervertex.
 *
 * Please Note: this implementation induces hypervertices in order
 * to mimic the Reduce operation over the VertexFusion binary operator.
 * Please note that, by applying reduce over VertexFusion, you
 * won't logically obtain the same result. This should be considered
 * as a mere generalization.
 *
 */
public class ReduceVertexFusionBiBroadcast implements GraphGraphGraphCollectionToGraph {

  @Override
  public LogicalGraph execute(LogicalGraph left, LogicalGraph right,
    GraphCollection hypervertices) {
    LogicalGraph gU = new Combination().execute(left, right);

    // Missing in the theoric definition: creating a new header
    GradoopId newGraphid = GradoopId.get();
    DataSet<GraphHead> gh = gU.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    DataSet<GradoopId> subgraphIds = hypervertices.getGraphHeads().map(new Id<>());

    // PHASE 1: Induced Subgraphs
    // Associate each vertex to its graph id
    DataSet<Tuple2<Vertex, GradoopId>> vWithGid = hypervertices.getVertices()
      .filter(new InBiGraphBroadcast<>())
      .withBroadcastSet(left.getGraphHead().map(new Id<>()), GRAPH_LEFT)
      .withBroadcastSet(right.getGraphHead().map(new Id<>()), GRAPH_RIGHT)
      .flatMap(new MapVertexToPairWithGraphId());

    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<Tuple2<Vertex, GradoopId>> nuWithGid  = hypervertices.getGraphHeads()
      .map(new CoGroupGraphHeadToVertex());

    // PHASE 2: Recreating the vertices
    DataSet<Vertex> vi = gU.getVertices()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<Vertex> vToRet = nuWithGid
      .map(new Value0Of2<>())
      .union(vi)
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    // PHASE 3: Recreating the edges
    DataSet<Tuple2<Vertex, GradoopId>> idJoin = vWithGid
      .coGroup(nuWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new CoGroupAssociateOldVerticesWithNewIds())
      .union(vi.map(new MapVerticesAsTuplesWithNullId()));

    DataSet<Edge> edges = gU.getEdges()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS)
      .leftOuterJoin(idJoin)
      .where(new SourceId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(true))
      .leftOuterJoin(idJoin)
      .where(new TargetId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(false))
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    return LogicalGraph.fromDataSets(gh, vToRet, edges, gU.getConfig());

  }

  @Override
  public String getName() {
    return ReduceVertexFusionBiBroadcast.class.getName();
  }
}
