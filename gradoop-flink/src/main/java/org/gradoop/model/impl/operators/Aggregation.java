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

import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.operators.UnaryGraphToGraphOperator;

public class Aggregation<O extends Number> implements
  UnaryGraphToGraphOperator {

  private final String aggregatePropertyKey;
  private final UnaryFunction<EPGraph, O> aggregationFunc;

  public Aggregation(final String aggregatePropertyKey,
    UnaryFunction<EPGraph, O> aggregationFunc) {

    this.aggregatePropertyKey = aggregatePropertyKey;
    this.aggregationFunc = aggregationFunc;
  }

  @Override
  public EPGraph execute(EPGraph graph) throws Exception {
    O result = aggregationFunc.execute(graph);
    // copy graph data before updating properties
    EPFlinkGraphData newGraphData =
      new EPFlinkGraphData(graph.getId(), graph.getLabel(),
        graph.getProperties());
    newGraphData.setProperty(aggregatePropertyKey, result);
    return EPGraph.fromGraph(graph.getGellyGraph(), newGraphData);
  }

  @Override
  public String getName() {
    return "Aggregation";
  }
}
