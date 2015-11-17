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
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.model.impl.algorithms.labelpropagation.pojos.LPVertexValue;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collections;
import java.util.List;

/**
 * Updates the value of a vertex by picking the minimum neighbor ID out of
 * all the incoming messages.
 */
public class LPUpdateFunction extends
  VertexUpdateFunction<GradoopId, LPVertexValue, GradoopId> {
  /**
   * {@inheritDoc}
   */
  @Override
  public void updateVertex(Vertex<GradoopId, LPVertexValue> vertex,
    MessageIterator<GradoopId> msg) throws Exception {
    if (getSuperstepNumber() == 1) {
      //Todo: Use Broadcast to set ChangeMax
      vertex.getValue().setChangeMax(20);
      vertex.getValue().setCurrentCommunity(vertex.getId());
      setNewVertexValue(vertex.getValue());
    } else {
      GradoopId currentCommunity = vertex.getValue().getCurrentCommunity();
      GradoopId lastCommunity = vertex.getValue().getLastCommunity();
      int stabilizationRound = vertex.getValue().getStabilizationCounter();
      GradoopId newCommunity = getNewCommunity(vertex, msg);
      boolean changed = ! currentCommunity.equals(newCommunity);
      boolean lastEqualsNew = lastCommunity.equals(newCommunity);
      if (changed && lastEqualsNew) {
        //Counts the amount of community swaps between 2 communities
        stabilizationRound++;
        vertex.getValue().setStabilizationCounter(stabilizationRound);
        boolean maximalChanges =
          stabilizationRound <= vertex.getValue().getChangeMax();
        if (maximalChanges) {
          vertex.getValue().setLastCommunity(currentCommunity);
          vertex.getValue().setCurrentCommunity(newCommunity);
          setNewVertexValue(vertex.getValue());
        } else {
          vertex.getValue()
            .setCurrentCommunity(GradoopId.min(currentCommunity, newCommunity));
          vertex.getValue()
            .setLastCommunity(vertex.getValue().getCurrentCommunity());
          setNewVertexValue(vertex.getValue());
        }
      }
      if (changed && !lastEqualsNew) {
        vertex.getValue().setLastCommunity(currentCommunity);
        vertex.getValue().setCurrentCommunity(newCommunity);
        setNewVertexValue(vertex.getValue());
      }
    }
  }

  /**
   * Returns the current new value. This value is based on all incoming
   * messages. Depending on the number of messages sent to the vertex, the
   * method returns:
   * <p/>
   * 0 messages:   The current value
   * <p/>
   * 1 message:    The minimum of the message and the current vertex value
   * <p/>
   * >1 messages:  The most frequent of all message values
   *
   * @param vertex The current vertex
   * @param msg    All incoming messages
   * @return the new Value the vertex will become
   */
  private GradoopId getNewCommunity(Vertex<GradoopId, LPVertexValue> vertex,
    MessageIterator<GradoopId> msg) {
    GradoopId newCommunity;
    List<GradoopId> allMessages = Lists.newArrayList(msg.iterator());
    GradoopId currentCommunity = vertex.getValue().getCurrentCommunity();
    if (allMessages.isEmpty()) {
      // 1. if no messages are received
      newCommunity = currentCommunity;
    } else if (allMessages.size() == 1) {
      // 2. if just one message are received

      GradoopId firstMessage = allMessages.get(0);
      newCommunity = GradoopId.min(firstMessage, currentCommunity);
    } else {
      // 3. if multiple messages are received
      newCommunity = getMostFrequent(vertex, allMessages);
    }
    return newCommunity;
  }

  /**
   * Returns the most frequent value based on all received messages.
   *
   * @param vertex      the current vertex
   * @param allMessages all received messages
   * @return most frequent value below all messages
   */
  private GradoopId getMostFrequent(Vertex<GradoopId, LPVertexValue> vertex,
    List<GradoopId> allMessages) {
    Collections.sort(allMessages);
    GradoopId newValue;
    int currentCounter = 1;
    GradoopId firstMessage = allMessages.get(0);
    GradoopId currentValue = firstMessage;
    int maxCounter = 1;
    GradoopId maxValue = GradoopId.fromLong(1L);
    for (int i = 1; i < allMessages.size(); i++) {
      if (currentValue == allMessages.get(i)) {
        currentCounter++;
        if (maxCounter < currentCounter) {
          maxCounter = currentCounter;
          maxValue = currentValue;
        }
      } else {
        currentCounter = 1;
        currentValue = allMessages.get(i);
      }
    }
    // if the frequent of all received messages are just one
    if (maxCounter == 1) {
      // to avoid an oscillating state of the calculation we will just use
      // the smaller value
      GradoopId currentCommunity = vertex.getValue().getCurrentCommunity();
      newValue = GradoopId.min(firstMessage, currentCommunity);
    } else {
      newValue = maxValue;
    }
    return newValue;
  }
}
