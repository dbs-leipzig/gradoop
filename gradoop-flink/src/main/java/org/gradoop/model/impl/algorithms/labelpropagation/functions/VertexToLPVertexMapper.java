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

package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;

/**
 * Maps EPGM vertices to a Label Propagation specific representation.
 *
 * @param <VD> EPGM vertex type
 * @see LPVertexValue
 */
public class VertexToLPVertexMapper<VD extends VertexData>
  implements MapFunction<VD, Vertex<Long, LPVertexValue>> {

  @Override
  public Vertex<Long, LPVertexValue> map(VD vertex) throws Exception {
    return new Vertex<>(vertex.getId(),
      new LPVertexValue(vertex.getId(), vertex.getId()));
  }
}
