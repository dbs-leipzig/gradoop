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

package org.gradoop.model.impl.algorithms.epgmlabelpropagation.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.model.api.EPGMVertex;

import java.util.Collections;
import java.util.List;

import static org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm.CURRENT_VALUE;
import static org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm.LAST_VALUE;
import static org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm.STABILIZATION_COUNTER;
import static org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm.STABILIZATION_MAX;


/**
 * Updates the value of a vertex by picking the minimum neighbor ID out of
 * all the incoming messages.
 *
 * @param <VD> EPGM vertex type
 */
public class LPUpdateFunction<VD extends EPGMVertex>
  extends VertexUpdateFunction<Long, VD, Long> {
  @Override
  public void updateVertex(Vertex<Long, VD> vertex,
    MessageIterator<Long> msg) throws Exception {
    if (getSuperstepNumber() == 1) {
      vertex.getValue().setProperty(CURRENT_VALUE, vertex.getId());
      vertex.getValue().setProperty(LAST_VALUE, Long.MAX_VALUE);
      vertex.getValue().setProperty(STABILIZATION_COUNTER, 0);
      //Todo: Use Broadcast to set ChangeMax
      vertex.getValue().setProperty(STABILIZATION_MAX, 20);
      setNewVertexValue(vertex.getValue());
    } else {
      long currentCommunity =
        (Long) vertex.getValue().getProperty(CURRENT_VALUE);
      long lastCommunity = (Long) vertex.getValue().getProperty(LAST_VALUE);
      int stabilizationRound =
        (int) vertex.getValue().getProperty(STABILIZATION_COUNTER);
      long newCommunity = getNewCommunity(vertex, msg);
      boolean changed = currentCommunity != newCommunity;
      boolean lastEqualsnew = lastCommunity == newCommunity;
      if (changed &&
        lastEqualsnew) { //Counts the amount of community swaps between 2
        // communities
        stabilizationRound++;
        vertex.getValue()
          .setProperty(STABILIZATION_COUNTER, stabilizationRound);
        boolean maximalChanges = stabilizationRound <=
          (int) vertex.getValue().getProperty(STABILIZATION_MAX);
        if (maximalChanges) {
          vertex.getValue().setProperty(LAST_VALUE, currentCommunity);
          vertex.getValue().setProperty(CURRENT_VALUE, newCommunity);
          setNewVertexValue(vertex.getValue());
        } else {
          vertex.getValue().setProperty(CURRENT_VALUE,
            Math.min(currentCommunity, newCommunity));
          vertex.getValue().setProperty(LAST_VALUE,
            vertex.getValue().getProperty(CURRENT_VALUE));
          setNewVertexValue(vertex.getValue());
        }
      }
      if (changed && !lastEqualsnew) {
        vertex.getValue().setProperty(LAST_VALUE, currentCommunity);
        vertex.getValue().setProperty(CURRENT_VALUE, newCommunity);
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
  private long getNewCommunity(Vertex<Long, VD> vertex,
    MessageIterator<Long> msg) {
    long newCommunity;
    List<Long> allMessages = Lists.newArrayList(msg.iterator());
    if (allMessages.isEmpty()) {
      // 1. if no messages are received
      newCommunity = (Long) vertex.getValue().getProperty(CURRENT_VALUE);
    } else if (allMessages.size() == 1) {
      // 2. if just one message are received
      newCommunity = Math
        .min((Long) vertex.getValue().getProperty(CURRENT_VALUE),
          allMessages.get(0));
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
  private long getMostFrequent(Vertex<Long, VD> vertex,
    List<Long> allMessages) {
    Collections.sort(allMessages);
    long newValue;
    int currentCounter = 1;
    long currentValue = allMessages.get(0);
    int maxCounter = 1;
    long maxValue = 1;
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
      newValue = Math.min((Long) vertex.getValue().getProperty(CURRENT_VALUE),
        allMessages.get(0));
    } else {
      newValue = maxValue;
    }
    return newValue;
  }
}
