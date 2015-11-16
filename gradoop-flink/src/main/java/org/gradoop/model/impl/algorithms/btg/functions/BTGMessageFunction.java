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

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGMessage;
import org.gradoop.model.impl.algorithms.btg.utils.BTGVertexType;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.gradoop.model.impl.id.GradoopId;

/**
 * BTG specific messaging function.
 */
public final class BTGMessageFunction extends
  MessagingFunction<GradoopId, BTGVertexValue, BTGMessage, NullValue> {
  /**
   * {@inheritDoc}
   */
  @Override
  public void sendMessages(Vertex<GradoopId, BTGVertexValue> vertex) throws
    Exception {
    if (vertex.getValue().getVertexType() == BTGVertexType.TRANSACTIONAL) {
      BTGMessage message = new BTGMessage();
      message.setSenderID(vertex.getId());
      if (vertex.getValue().getLastGraph() != null) {
        message.setBtgID(vertex.getValue().getLastGraph());
      }
      sendMessageToAllNeighbors(message);
    }
  }
}
