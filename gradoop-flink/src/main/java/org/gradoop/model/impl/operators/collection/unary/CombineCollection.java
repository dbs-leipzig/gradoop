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

import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.util.FlinkConstants;

/**
 * Combines all logical graphs of a GraphCollection into a single LogicalGraph.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class CombineCollection<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  UnaryCollectionToGraphOperator<VD, ED, GD> {
  @Override
  public LogicalGraph<VD, ED, GD> execute(
    GraphCollection<VD, ED, GD> collection) {
    return LogicalGraph
      .fromDataSets(collection.getVertices(), collection.getEdges(),
        collection.getConfig().getGraphHeadFactory()
          .createGraphData(FlinkConstants.COMBINE_GRAPH_ID),
        collection.getConfig());
  }

  @Override
  public String getName() {
    return CombineCollection.class.getName();
  }
}
