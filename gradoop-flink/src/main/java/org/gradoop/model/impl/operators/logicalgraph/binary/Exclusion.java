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

package org.gradoop.model.impl.operators.logicalgraph.binary;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeSourceVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeTargetVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.functions.mapfunctions.EdgeToGraphUpdater;
import org.gradoop.model.impl.functions.mapfunctions.VertexToGraphUpdater;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.FlinkConstants;

/**
 * Creates a new logical graph containing only vertices and edges that
 * exist in the first input graph but not in the second input graph. Vertex and
 * edge equality is based on their respective identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class Exclusion<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends AbstractBinaryGraphToGraphOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<GD, VD, ED> executeInternal(
    LogicalGraph<GD, VD, ED> firstGraph, LogicalGraph<GD, VD, ED> secondGraph) {
    final GradoopId newGraphID = FlinkConstants.EXCLUDE_GRAPH_ID;

    // union vertex sets, group by vertex id, filter vertices where the group
    // contains exactly one vertex which belongs to the graph, the operator is
    // called on
    DataSet<VD> newVertexSet = firstGraph.getVertices()
      .union(secondGraph.getVertices())
      .groupBy(new VertexKeySelector<VD>())
      .reduceGroup(
        new VertexGroupReducer<VD>(1L, firstGraph.getId(), secondGraph.getId()))
      .map(new VertexToGraphUpdater<VD>(newGraphID));

    JoinFunction<ED, VD, ED> joinFunc = new JoinFunction<ED, VD, ED>() {
        @Override
        public ED join(ED leftEdge,
          VD rightVertex) throws Exception {
          return leftEdge;
        }
      };

    // In exclude(), we are only interested in edges that connect vertices
    // that are in the exclusion of the vertex sets. Thus, we join the edges
    // from the left graph with the new vertex set using source and target ids.
    DataSet<ED> newEdgeSet = firstGraph.getEdges()
      .join(newVertexSet)
      .where(new EdgeSourceVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>()).with(joinFunc)
      .join(newVertexSet)
      .where(new EdgeTargetVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>()).with(joinFunc)
      .map(new EdgeToGraphUpdater<ED>(newGraphID));

    return LogicalGraph.fromDataSets(newVertexSet, newEdgeSet,
      firstGraph.getConfig().getGraphHeadFactory().initGraphHead(newGraphID),
      firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Exclusion.class.getName();
  }
}
