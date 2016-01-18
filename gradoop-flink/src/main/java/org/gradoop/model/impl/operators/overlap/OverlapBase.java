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

package org.gradoop.model.impl.operators.overlap;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Base class for overlap operators that contains common logic.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @see Overlap
 * @see ReduceOverlap
 */
public abstract class OverlapBase
  <V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Filters vertices based on the given graph identifiers.
   *
   * @param vertices  vertices
   * @param ids       graph identifiers
   * @return filtered vertices
   */
  protected DataSet<V> getVertices(DataSet<V> vertices,
    DataSet<GradoopId> ids) {
    return vertices
      .filter(new InAllGraphsBroadcast<V>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }

  /**
   * Filters edges based on the given graph identifiers.
   *
   * @param edges edges
   * @param ids   graph identifiers
   * @return filtered edges
   */
  protected DataSet<E> getEdges(DataSet<E> edges, DataSet<GradoopId> ids) {
    return edges
      .filter(new InAllGraphsBroadcast<E>())
      .withBroadcastSet(ids, InAllGraphsBroadcast.GRAPH_IDS);
  }
}
