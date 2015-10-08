/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.btg.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EdgeData;

/**
 * Maps EPGM edges to a BTG specific representation.
 *
 * @param <ED> edge data type
 */
public class EdgeToBTGEdgeMapper<ED extends EdgeData> implements
  MapFunction<Edge<Long, ED>, Edge<Long, NullValue>> {
  @Override
  public Edge<Long, NullValue> map(Edge<Long, ED> edge) throws Exception {
    return new Edge<>(edge.getSource(), edge.getTarget(),
      NullValue.getInstance());
  }
}
