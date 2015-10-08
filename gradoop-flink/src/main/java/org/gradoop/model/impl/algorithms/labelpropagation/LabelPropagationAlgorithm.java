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
package org.gradoop.model.impl.algorithms.labelpropagation;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.types.NullValue;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the Label Propagation Algorithm:
 * The input graph as adjacency list contains the information about the
 * vertex (id), value (label) and its edges to neighbors.
 * <p/>
 * In super step 0 each vertex will propagate its value within his neighbors
 * <p/>
 * In the remaining super steps each vertex will adopt the value of the
 * majority of their neighbors or the smallest one if there are just one
 * neighbor. If a vertex adopt a new value it'll propagate the new one again.
 * <p/>
 * The computation will terminate if no new values are assigned.
 */
public class LabelPropagationAlgorithm implements
  GraphAlgorithm<Long, LabelPropagationValue, NullValue,
    Graph<Long, LabelPropagationValue, NullValue>> {
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int counter to define maximal Iterations
   */
  public LabelPropagationAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * Graph run method to start the VertexCentricIteration
   *
   * @param graph graph that should be used for EPGMLabelPropagation
   * @return gelly Graph with labeled vertices
   * @throws Exception
   */
  @Override
  public Graph<Long, LabelPropagationValue, NullValue> run(
    Graph<Long, LabelPropagationValue, NullValue> graph) throws Exception {
    // initialize vertex values and run the Vertex Centric Iteration
    Graph<Long, LabelPropagationValue, NullValue> gellyGraph =
      graph.getUndirected();
    return gellyGraph
      .runVertexCentricIteration(new LPUpdater(), new LPMessenger(),
        maxIterations);
  }

  /**
   * Updates the value of a vertex by picking the minimum neighbor ID out of
   * all the incoming messages.
   */
  public static final class LPUpdater extends
    VertexUpdateFunction<Long, LabelPropagationValue, Long> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void updateVertex(Vertex<Long, LabelPropagationValue> vertex,
      MessageIterator<Long> msg) throws Exception {
      if (getSuperstepNumber() == 1) {
        //Todo: Use Broadcast to set ChangeMax
        vertex.getValue().setChangeMax(20);
        vertex.getValue().setCurrentCommunity(vertex.getId());
        setNewVertexValue(vertex.getValue());
      } else {
        long currentCommunity = vertex.getValue().getCurrentCommunity();
        long lastCommunity = vertex.getValue().getLastCommunity();
        int stabilizationRound = vertex.getValue().getStabilizationCounter();
        long newCommunity = getNewCommunity(vertex, msg);
        boolean changed = currentCommunity != newCommunity;
        boolean lastEqualsNew = lastCommunity == newCommunity;
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
              .setCurrentCommunity(Math.min(currentCommunity, newCommunity));
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
    private long getNewCommunity(Vertex<Long, LabelPropagationValue> vertex,
      MessageIterator<Long> msg) {
      long newCommunity;
      List<Long> allMessages = Lists.newArrayList(msg.iterator());
      if (allMessages.isEmpty()) {
        // 1. if no messages are received
        newCommunity = vertex.getValue().getCurrentCommunity();
      } else if (allMessages.size() == 1) {
        // 2. if just one message are received
        newCommunity =
          Math.min(vertex.getValue().getCurrentCommunity(), allMessages.get(0));
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
    private long getMostFrequent(Vertex<Long, LabelPropagationValue> vertex,
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
        newValue =
          Math.min(vertex.getValue().getCurrentCommunity(), allMessages.get(0));
      } else {
        newValue = maxValue;
      }
      return newValue;
    }
  }

  /**
   * Distributes the value of the vertex
   */
  public static final class LPMessenger extends
    MessagingFunction<Long, LabelPropagationValue, Long, NullValue> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessages(Vertex<Long, LabelPropagationValue> vertex) throws
      Exception {
      // send current minimum to neighbors
      sendMessageToAllNeighbors(vertex.getValue().getCurrentCommunity());
    }
  }
}
