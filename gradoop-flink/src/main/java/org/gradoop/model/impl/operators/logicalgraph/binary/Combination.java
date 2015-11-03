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

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.functions.mapfunctions.EdgeToGraphUpdater;
import org.gradoop.model.impl.functions.mapfunctions.VertexToGraphUpdater;
import org.gradoop.util.FlinkConstants;

/**
 * Creates a new logical graph by combining the vertex and edge sets of two
 * input graphs. Vertex and edge equality is based on their
 * respective identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class Combination<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  extends AbstractBinaryGraphToGraphOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<VD, ED, GD> executeInternal(
    LogicalGraph<VD, ED, GD> firstGraph, LogicalGraph<VD, ED, GD> secondGraph) {
    final Long newGraphID = FlinkConstants.COMBINE_GRAPH_ID;

    // build distinct union of vertex sets and update graph ids at vertices
    // cannot use Gelly union here because of missing argument for KeySelector
    DataSet<VD> newVertexSet = firstGraph.getVertices()
      .union(secondGraph.getVertices())
      .distinct(new VertexKeySelector<VD>())
      .map(new VertexToGraphUpdater<VD>(newGraphID));

    DataSet<ED> newEdgeSet = firstGraph.getEdges()
      .union(secondGraph.getEdges())
      .distinct(new EdgeKeySelector<ED>())
      .map(new EdgeToGraphUpdater<ED>(newGraphID));

    return LogicalGraph.fromDataSets(newVertexSet, newEdgeSet,
      firstGraph.getConfig().getGraphHeadFactory().createGraphData(newGraphID),
      firstGraph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Combination.class.getName();
  }
}
