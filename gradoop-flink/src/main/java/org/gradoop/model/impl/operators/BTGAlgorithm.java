package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.model.impl.operators.io.formats.BTGVertexType;
import org.gradoop.model.impl.operators.io.formats.BTGVertexValue;

import java.util.List;

/**
 * Flink based BTGComputation
 */
public class BTGAlgorithm implements
  GraphAlgorithm<Long, BTGVertexValue, NullValue> {
  /**
   * Max Iteration counter for the Algorithm
   */
  private int maxIterations;

  /**
   * Constructor
   *
   * @param maxIterations maximal Iterations of Algorithm
   */
  public BTGAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph<Long, BTGVertexValue, NullValue> run(
    Graph<Long, BTGVertexValue, NullValue> graph) throws Exception {
    Graph<Long, BTGVertexValue, NullValue> undirectedGraph =
      graph.getUndirected();
    // initialize vertex values and run the Vertex Centric Iteration
    return undirectedGraph
      .runVertexCentricIteration(new BTGUpdater(), new BTGMessage(),
        maxIterations);
  }

  /**
   * Vertex Updater Class
   */
  private static final class BTGUpdater extends
    VertexUpdateFunction<Long, BTGVertexValue, org.gradoop.model.impl
      .operators.io.formats.BTGMessage> {
    @Override
    public void updateVertex(Vertex<Long, BTGVertexValue> vertex,
      MessageIterator<org.gradoop.model.impl.operators.io.formats.BTGMessage>
        messages) throws
      Exception {
      if (vertex.getValue().getVertexType() == BTGVertexType.MASTER) {
        processMasterVertex(vertex, messages);
      } else if (vertex.getValue().getVertexType() ==
        BTGVertexType.TRANSACTIONAL) {
        long currentMinValue = getCurrentMinValue(vertex);
        long newMinValue = getNewMinValue(messages, currentMinValue);
        boolean changed = currentMinValue != newMinValue;
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
    private void processMasterVertex(Vertex<Long, BTGVertexValue> vertex,
      MessageIterator<org.gradoop.model.impl.operators.io.formats.BTGMessage>
        messages) {
      BTGVertexValue vertexValue = vertex.getValue();
      if (getSuperstepNumber() > 1) {
        for (org.gradoop.model.impl.operators.io.formats.BTGMessage message :
          messages) {
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
    private void processTransactionalVertex(Vertex<Long, BTGVertexValue> vertex,
      long minValue) {
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
    private long getNewMinValue(
      MessageIterator<org.gradoop.model.impl.operators.io.formats.BTGMessage>
        messages,
      long currentMinValue) {
      long newMinValue = currentMinValue;
      if (getSuperstepNumber() > 1) {
        for (org.gradoop.model.impl.operators.io.formats.BTGMessage message :
          messages) {
          if (message.getBtgID() < newMinValue) {
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
    private long getCurrentMinValue(Vertex<Long, BTGVertexValue> vertex) {
      List<Long> btgIDs = Lists.newArrayList(vertex.getValue().getGraphs());
      return (btgIDs.size() > 0) ? btgIDs.get(btgIDs.size() - 1) :
        vertex.getId();
    }
  }

  /**
   * Vertex Message Class
   */
  private static final class BTGMessage extends
    MessagingFunction<Long, BTGVertexValue, org.gradoop.model.impl.operators
      .io.formats.BTGMessage, NullValue> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessages(Vertex<Long, BTGVertexValue> vertex) throws
      Exception {
      if (vertex.getValue().getVertexType() == BTGVertexType.TRANSACTIONAL) {
        org.gradoop.model.impl.operators.io.formats.BTGMessage message =
          new org.gradoop.model.impl.operators.io.formats.BTGMessage();
        message.setSenderID(vertex.getId());
        if (vertex.getValue().getLastGraph() != null) {
          message.setBtgID(vertex.getValue().getLastGraph());
        }
        sendMessageToAllNeighbors(message);
      }
    }
  }
}
