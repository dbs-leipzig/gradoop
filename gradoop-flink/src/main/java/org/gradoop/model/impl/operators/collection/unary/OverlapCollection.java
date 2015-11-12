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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.collection.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.filterfunctions
  .EdgeInAllGraphsFilterWithBC;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInAllGraphsFilterWithBC;
import org.gradoop.model.impl.functions.mapfunctions.GraphToIdentifierMapper;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.FlinkConstants;

/**
 * Intersects all subgraphs of a GraphCollection and returns the result
 * as new logical graph.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class OverlapCollection<VD extends EPGMVertex, ED extends EPGMEdge, GD
  extends EPGMGraphHead> implements
  UnaryCollectionToGraphOperator<VD, ED, GD> {
  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(
    GraphCollection<VD, ED, GD> collection) {
    DataSet<GD> graphHeads = collection.getGraphHeads();
    DataSet<GradoopId> graphIDs =
      graphHeads.map(new GraphToIdentifierMapper<GD>());
    DataSet<VD> vertices =
      collection.getVertices().filter(new VertexInAllGraphsFilterWithBC<VD>())
        .withBroadcastSet(graphIDs,
          VertexInAllGraphsFilterWithBC.BC_IDENTIFIERS);
    DataSet<ED> edges =
      collection.getEdges().filter(new EdgeInAllGraphsFilterWithBC<ED>())
        .withBroadcastSet(graphIDs, EdgeInAllGraphsFilterWithBC.BC_IDENTIFIERS);
    return LogicalGraph.fromDataSets(vertices, edges,
      collection.getConfig().getGraphHeadFactory()
        .createGraphHead(FlinkConstants.OVERLAP_GRAPH_ID),
      collection.getConfig());
  }

  @Override
  public String getName() {
    return OverlapCollection.class.getName();
  }
}
