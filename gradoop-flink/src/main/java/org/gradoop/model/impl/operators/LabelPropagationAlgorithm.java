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
package org.gradoop.model.impl.operators;

import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of the Label Propagation Algorithm:
 * The input graph as adjacency list contains the information about the
 * vertex (id), value (label) and its edges to neighbors.
 * <p>
 * In super step 0 each vertex will propagate its value within his neighbors
 * <p>
 * In the remaining super steps each vertex will adopt the value of the
 * majority of their neighbors or the smallest one if there are just one
 * neighbor. If a vertex adopt a new value it'll propagate the new one again.
 * <p>
 * The computation will terminate if no new values are assigned.
 */
public class LabelPropagationAlgorithm implements
  GraphAlgorithm<Long, EPFlinkVertexData, EPFlinkEdgeData> {
  /**
   * Vertex property key where the resulting label is stored.
   */
  public static final String PROPERTY_KEY = "value";
  /**
   * Counter to define maximal Iteration for the Algorithm
   */
  private Integer maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations int counter to define maximal Iterations
   */
  public LabelPropagationAlgorithm(Integer maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * Graph run method to start the VertexCentricIteration
   *
   * @param graph graph that should be used for LabelPropagation
   * @return gelly Graph with labeled vertices
   * @throws Exception
   */
  @Override
  public Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> run(
    Graph<Long, EPFlinkVertexData, EPFlinkEdgeData> graph) throws Exception {
    // initialize vertex values and run the Vertex Centric Iteration
    return graph.runVertexCentricIteration(new LPUpdater(), new LPMessenger(),
      maxIterations);
  }

  /**
   * Updates the value of a vertex by picking the minimum neighbor ID out of
   * all the incoming messages.
   */
  public static final class LPUpdater extends
    VertexUpdateFunction<Long, EPFlinkVertexData, Long> {
    @Override
    public void updateVertex(Vertex<Long, EPFlinkVertexData> vertex,
      MessageIterator<Long> msg) throws Exception {
      if (getSuperstepNumber() == 1) {
        setNewVertexValue(vertex.getValue());
      } else {
        List<Long> messages = Lists.newArrayList(msg.iterator());
        if (messages.size() == 1) {
          vertex.getValue().setProperty(PROPERTY_KEY, Math
            .min((Long) vertex.getValue().getProperty(PROPERTY_KEY),
              messages.get(0)));
          setNewVertexValue(vertex.getValue());
        } else {
          vertex.getValue()
            .setProperty(PROPERTY_KEY, getMostFrequent(vertex, messages));
          setNewVertexValue(vertex.getValue());
        }
      }
    }

    /**
     * Returns the most frequent value based on all received messages.
     *
     * @param vertex      the current vertex
     * @param allMessages all received messages
     * @return most frequent value below all messages
     */
    private long getMostFrequent(Vertex<Long, EPFlinkVertexData> vertex,
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
        newValue = Math.min((Long) vertex.getValue().getProperty(PROPERTY_KEY),
          allMessages.get(0));
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
    MessagingFunction<Long, EPFlinkVertexData, Long, EPFlinkEdgeData> {
    @Override
    public void sendMessages(Vertex<Long, EPFlinkVertexData> vertex) throws
      Exception {
      // send current minimum to neighbors
      sendMessageToAllNeighbors(
        (Long) vertex.getValue().getProperty(PROPERTY_KEY));
    }
  }
}
