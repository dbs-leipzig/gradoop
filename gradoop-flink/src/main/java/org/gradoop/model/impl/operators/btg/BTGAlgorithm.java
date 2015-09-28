package org.gradoop.model.impl.operators.btg;

import com.google.common.collect.Lists;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;

import java.util.List;

/**
 * Graph-BSP Implementation of the Business Transaction Graph (BTG) Extraction
 * algorithm described in "BIIIG: Enabling Business Intelligence with Integrated
 * Instance Graphs". The input graph is a so called Integrated Instance Graph
 * (IIG) which contains nodes belonging to two different classes: master data
 * and transactional data.
 * <p/>
 * A BTG is a sub-graph of the IIG which has only master data nodes as boundary
 * nodes and transactional data nodes as inner nodes. The algorithm finds all
 * BTGs inside a given IIG. In the business domain, a BTG describes a specific
 * case inside a set of business cases involving master data like Employees,
 * Customers and Products and transactional data like SalesOrders, ProductOffers
 * or Purchases.
 * <p/>
 * The algorithm is based on the idea of finding connected components by
 * communicating the minimum vertex id inside a connected sub-graph and storing
 * it. The minimum id inside a sub-graph is the BTG id. Only transactional data
 * nodes are allowed to send ids, so the master data nodes work as a
 * communication barrier between BTGs. The master data nodes receive messages
 * from transactional data nodes out of BTGs in which they are involved. They
 * store the minimum incoming BTG id by vertex id in a map and when the
 * algorithm terminates, the set of unique values inside this map is the set of
 * BTG ids the master data node is involved in.
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
      .operators.btg.BTGMessage> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void updateVertex(Vertex<Long, BTGVertexValue> vertex,
      MessageIterator<org.gradoop.model.impl.operators.btg.BTGMessage>
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
      MessageIterator<org.gradoop.model.impl.operators.btg.BTGMessage>
        messages) {
      BTGVertexValue vertexValue = vertex.getValue();
      if (getSuperstepNumber() > 1) {
        for (org.gradoop.model.impl.operators.btg.BTGMessage message :
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
      MessageIterator<org.gradoop.model.impl.operators.btg.BTGMessage> messages,
      long currentMinValue) {
      long newMinValue = currentMinValue;
      if (getSuperstepNumber() > 1) {
        for (org.gradoop.model.impl.operators.btg.BTGMessage message :
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
      .btg.BTGMessage, NullValue> {
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessages(Vertex<Long, BTGVertexValue> vertex) throws
      Exception {
      if (vertex.getValue().getVertexType() == BTGVertexType.TRANSACTIONAL) {
        org.gradoop.model.impl.operators.btg.BTGMessage message =
          new org.gradoop.model.impl.operators.btg.BTGMessage();
        message.setSenderID(vertex.getId());
        if (vertex.getValue().getLastGraph() != null) {
          message.setBtgID(vertex.getValue().getLastGraph());
        }
        sendMessageToAllNeighbors(message);
      }
    }
  }
}
