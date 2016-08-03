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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Base class for overlap operators that contains common logic.
 *
 * @see Overlap
 * @see ReduceOverlap
 */
public abstract class OverlapBase {

  /**
   * Filters vertices based on the given graph identifiers.
   *
   * @param vertices  vertices
   * @param ids       graph identifiers
   * @return filtered vertices
   */
  protected DataSet<Vertex> getVertices(DataSet<Vertex> vertices,
    DataSet<GradoopId> ids) {
    return vertices
      .filter(new InAllGraphsBroadcast<Vertex>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }

  /**
   * Filters edges based on the given graph identifiers.
   *
   * @param edges edges
   * @param ids   graph identifiers
   * @return filtered edges
   */
  protected DataSet<Edge> getEdges(DataSet<Edge> edges,
    DataSet<GradoopId> ids) {
    return edges
      .filter(new InAllGraphsBroadcast<Edge>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }
}
