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

/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or transform
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

package org.gradoop.flink.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Computes the overlap graph from a collection of logical graphs.
 */
public class ReduceOverlap extends OverlapBase implements
  ReducibleBinaryGraphToGraphOperator {

  /**
   * Creates a new logical graph containing the overlapping vertex and edge sets
   * of the graphs contained in the given collection. Vertex and edge equality
   * is based on their respective identifiers.
   *
   * @param collection input collection
   * @return graph with overlapping elements from the input collection
   */
  @Override
  public LogicalGraph execute(GraphCollection collection) {
    DataSet<GraphHead> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads.map(new Id<GraphHead>());

    return LogicalGraph.fromDataSets(
      getVertices(collection.getVertices(), graphIDs),
      getEdges(collection.getEdges(), graphIDs),
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return ReduceOverlap.class.getName();
  }
}
