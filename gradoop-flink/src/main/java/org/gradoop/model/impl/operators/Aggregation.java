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

package org.gradoop.model.impl.operators;

import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.DefaultGraphDataFactory;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

public class Aggregation<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData, O extends Number> implements
  UnaryGraphToGraphOperator<VD, ED, GD> {

  private final String aggregatePropertyKey;
  private final UnaryFunction<EPGraph<VD, ED, GD>, O> aggregationFunc;

  public Aggregation(final String aggregatePropertyKey,
    UnaryFunction<EPGraph<VD, ED, GD>, O> aggregationFunc) {

    this.aggregatePropertyKey = aggregatePropertyKey;
    this.aggregationFunc = aggregationFunc;
  }

  @Override
  public EPGraph<VD, ED, GD> execute(EPGraph<VD, ED, GD> graph) throws
    Exception {
    O result = aggregationFunc.execute(graph);
    // copy graph data before updating properties
    GD newGraphData = graph.getGraphDataFactory()
      .createGraphData(graph.getId(), graph.getLabel());
    newGraphData.setProperties(graph.getProperties());
    newGraphData.setProperty(aggregatePropertyKey, result);
    return EPGraph.fromGraph(graph.getGellyGraph(), newGraphData,
      graph.getVertexDataFactory(), graph.getEdgeDataFactory(),
      graph.getGraphDataFactory());
  }

  @Override
  public String getName() {
    return "Aggregation";
  }
}
