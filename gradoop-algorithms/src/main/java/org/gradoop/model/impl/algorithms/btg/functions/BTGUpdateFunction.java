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
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGMessage;
import org.gradoop.model.impl.algorithms.btg.utils.BTGVertexType;
import org.gradoop.model.impl.algorithms.btg.pojos.BTGVertexValue;
import org.gradoop.model.impl.id.GradoopId;

import java.util.List;

/**
 * BTG specific vertex update function.
 */
public final class BTGUpdateFunction extends
  VertexUpdateFunction<GradoopId, BTGVertexValue, BTGMessage> {
  /**
   * {@inheritDoc}
   */
  @Override
  public void updateVertex(Vertex<GradoopId, BTGVertexValue> vertex,
    MessageIterator<BTGMessage>
      messages) throws
    Exception {
    if (vertex.getValue().getVertexType() == BTGVertexType.MASTER) {
      processMasterVertex(vertex, messages);
    } else if (vertex.getValue().getVertexType() ==
      BTGVertexType.TRANSACTIONAL) {
      GradoopId currentMinValue = getCurrentMinValue(vertex);
      GradoopId newMinValue = getNewMinValue(messages, currentMinValue);
      boolean changed = !currentMinValue.equals(newMinValue);
      if (getSuperstepNumber() == 1 || changed) {
        processTransactionalVertex(vertex, newMinValue);
      }
    }
  }

  /**
   * Processes vertices of type Master.
   *
   * @param vertex   The current vertex
   * @param messages All incoming messages
   */
  private void processMasterVertex(Vertex<GradoopId, BTGVertexValue> vertex,
    MessageIterator<BTGMessage>
      messages) {
    BTGVertexValue vertexValue = vertex.getValue();
    if (getSuperstepNumber() > 1) {
      for (BTGMessage message : messages) {
        vertexValue
          .updateNeighbourBtgID(message.getSenderID(), message.getBtgID());
      }
    }
    vertexValue.updateBtgIDs();
    // in case the vertex has no neighbours
    if (vertexValue.getGraphCount() == 0) {
      vertexValue.addGraph(vertex.getId());
    }
    setNewVertexValue(vertexValue);
  }

  /**
   * Processes vertices of type Transactional.
   *
   * @param vertex   The current vertex
   * @param minValue All incoming messages
   */
  private void processTransactionalVertex(
    Vertex<GradoopId, BTGVertexValue> vertex, GradoopId minValue) {

    vertex.getValue().removeLastBtgID();
    vertex.getValue().addGraph(minValue);
    setNewVertexValue(vertex.getValue());
  }

  /**
   * Checks incoming messages for smaller values than the current smallest
   * value.
   *
   * @param messages        All incoming messages
   * @param currentMinValue The current minimum value
   * @return The new (maybe unchanged) minimum value
   */
  private GradoopId getNewMinValue(MessageIterator<BTGMessage> messages,
    GradoopId currentMinValue) {
    GradoopId newMinValue = currentMinValue;
    if (getSuperstepNumber() > 1) {
      for (BTGMessage message : messages) {
        if (message.getBtgID().compareTo(newMinValue) < 0) {
          newMinValue = message.getBtgID();
        }
      }
    }
    return newMinValue;
  }

  /**
   * Returns the current minimum value. This is always the last value in the
   * list of BTG ids stored at this vertex. Initially the minimum value is the
   * vertex id.
   *
   * @param vertex The current vertex
   * @return The minimum BTG ID that vertex knows.
   */
  private GradoopId getCurrentMinValue(
    Vertex<GradoopId, BTGVertexValue> vertex) {

    List<GradoopId> btgIDs = vertex.getValue().getGraphs();
    return (btgIDs.size() > 0) ? btgIDs.get(btgIDs.size() - 1) :
      vertex.getId();
  }
}
