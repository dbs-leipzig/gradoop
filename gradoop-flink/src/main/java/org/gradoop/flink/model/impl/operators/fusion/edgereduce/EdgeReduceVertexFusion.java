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

package org.gradoop.flink.model.impl.operators.fusion.edgereduce;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.TernaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphBroadcast;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.tuples.VertexIdToEdgeId;

import static org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphContainmentFilterBroadcast.GRAPH_ID;

/**
 * [TODO]
 *
 */
public class EdgeReduceVertexFusion implements TernaryGraphToGraphOperator {


  @Override
  public LogicalGraph execute(LogicalGraph left, LogicalGraph right, LogicalGraph relationGraph) {
    // Union of the two graphs
    LogicalGraph gU = new Combination().execute(left, right);

    // Missing in the theoretic definition: creating a new header
    GradoopId newGraphid = GradoopId.get();
    DataSet<GraphHead> gh = gU.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    DataSet<VertexIdToEdgeId> verticesAssociatedWithHyperedges = relationGraph.getEdges()
      .flatMap(new FlatMapEdgesToVertices());

    DataSet<Vertex> notToBeFusedVertices = gU.getVertices()
      .filter(new NotInGraphBroadcast<>())
      .withBroadcastSet(relationGraph.getGraphHead().map(new Id<>()),GRAPH_ID);

    DataSet<Tuple2<GradoopId,GradoopId>> associateVertexToNullEdgeId = notToBeFusedVertices
      .map(new MapTuples)

    return LogicalGraph.fromDataSets(gh, verticesToBeReturned, edges, gU.getConfig());

  }

  @Override
  public String getName() {
    return EdgeReduceVertexFusion.class.getName();
  }
}
