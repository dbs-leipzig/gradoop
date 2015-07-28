package org.gradoop.model.impl.operators;

import com.google.common.collect.Lists;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGMessage;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexType;
import org.gradoop.model.impl.operators.io.formats.FlinkBTGVertexValue;

import java.util.List;

/**
 * BTGComputation
 */
public class FlinkBTGAlgorithm implements
  GraphAlgorithm<Long, FlinkBTGVertexValue, Long> {
  private int maxIterations;

  public FlinkBTGAlgorithm(int maxIterations) {
    this.maxIterations = maxIterations;
  }

  @Override
  public Graph<Long, FlinkBTGVertexValue, Long> run(
    Graph<Long, FlinkBTGVertexValue, Long> graph) throws Exception {
    Graph<Long, FlinkBTGVertexValue, Long> undirectedGraph =
      graph.getUndirected();
    // initialize vertex values and run the Vertex Centric Iteration
    return undirectedGraph
      .runVertexCentricIteration(new BTGUpdater(), new BTGMessage(),
        maxIterations);
  }

  private static final class BTGUpdater extends
    VertexUpdateFunction<Long, FlinkBTGVertexValue, FlinkBTGMessage> {
    @Override
    public void updateVertex(Vertex<Long, FlinkBTGVertexValue> vertex,
      MessageIterator<FlinkBTGMessage> messages) throws Exception {
      System.out.println("### VertexID: " + vertex.getId());
      if (vertex.getValue().getVertexType() == FlinkBTGVertexType.MASTER) {
        processMasterVertex(vertex, messages);
        System.out.println("### Master");
        System.out
          .println("### btgCount: " + vertex.getValue().getGraphCount());
      } else if (vertex.getValue().getVertexType() ==
        FlinkBTGVertexType.TRANSACTIONAL) {
        System.out.println("### Transactional");
        System.out
          .println("### btgCount: " + vertex.getValue().getGraphCount());
        long currentMinValue = getCurrentMinValue(vertex);
        long newMinValue = getNewMinValue(messages, currentMinValue);
        boolean changed = currentMinValue != newMinValue;
        //todo check superstep!
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
    private void processMasterVertex(Vertex<Long, FlinkBTGVertexValue> vertex,
      MessageIterator<FlinkBTGMessage> messages) {
      FlinkBTGVertexValue vertexValue = vertex.getValue();
      for (FlinkBTGMessage message : messages) {
        vertexValue
          .updateNeighbourBtgID(message.getSenderID(), message.getBtgID());
      }
      vertexValue.updateBtgIDs();
      // in case the vertex has no neighbours
      if (vertexValue.getGraphCount() == 0) {
        vertexValue.addGraph(vertex.getId());
      }
    }

    /**
     * Processes vertices of type Transactional.
     *
     * @param vertex   The current vertex
     * @param minValue All incoming messages
     */
    private void processTransactionalVertex(
      Vertex<Long, FlinkBTGVertexValue> vertex, long minValue) {
      vertex.getValue().removeLastBtgID();
      vertex.getValue().addGraph(minValue);
    }

    /**
     * Checks incoming messages for smaller values than the current smallest
     * value.
     *
     * @param messages        All incoming messages
     * @param currentMinValue The current minimum value
     * @return The new (maybe unchanged) minimum value
     */
    private long getNewMinValue(MessageIterator<FlinkBTGMessage> messages,
      long currentMinValue) {
      long newMinValue = currentMinValue;
      for (FlinkBTGMessage message : messages) {
        if (message.getBtgID() < newMinValue) {
          newMinValue = message.getBtgID();
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
    private long getCurrentMinValue(Vertex<Long, FlinkBTGVertexValue> vertex) {
      List<Long> btgIDs = Lists.newArrayList(vertex.getValue().getGraphs());
      return (btgIDs.size() > 0) ? btgIDs.get(btgIDs.size() - 1) :
        vertex.getId();
    }
  }

  private static final class BTGMessage extends
    MessagingFunction<Long, FlinkBTGVertexValue, FlinkBTGMessage, Long> {
    @Override
    public void sendMessages(Vertex<Long, FlinkBTGVertexValue> vertex) throws
      Exception {
      if (vertex.getValue().getVertexType() ==
        FlinkBTGVertexType.TRANSACTIONAL) {
        FlinkBTGMessage message = new FlinkBTGMessage();
        message.setSenderID(vertex.getId());
        if (vertex.getValue().getGraphCount() > 0) {
          message.setBtgID(vertex.getValue().getLastGraph());
        }
        System.out.println("send to all neighbors: ");
        System.out.println(message);
        sendMessageToAllNeighbors(message);
      }
    }
  }
}
