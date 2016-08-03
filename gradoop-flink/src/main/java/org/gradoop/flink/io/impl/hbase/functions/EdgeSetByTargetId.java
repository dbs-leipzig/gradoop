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

package org.gradoop.flink.io.impl.hbase.functions;

import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Takes grouped edges as input and outputs a tuple containing target vertex id
 * and the edges.
 *
 * edge+ -> (targetId, edge+)
 *
 * @param <E> EPGM edge type
 */
public class EdgeSetByTargetId<E extends Edge> extends EdgeSet<E> {

  /**
   * Constructor
   */
  public EdgeSetByTargetId() {
    extractBySourceId = false;
  }
}
