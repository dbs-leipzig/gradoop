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

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;

/**
 * Distributes the value of the vertex
 */
public class LPMessageFunction extends
  MessagingFunction<Long, LPVertexValue, Long, NullValue> {
  /**
   * {@inheritDoc}
   */
  @Override
  public void sendMessages(Vertex<Long, LPVertexValue> vertex) throws
    Exception {
    // send current minimum to neighbors
    sendMessageToAllNeighbors(vertex.getValue().getCurrentCommunity());
  }
}
