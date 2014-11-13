/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.io.IIGMessage;
import org.gradoop.io.IIGVertex;
import org.gradoop.io.IIGVertexType;

import java.io.IOException;
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
public class BTGComputation extends
  BasicComputation<LongWritable, IIGVertex, NullWritable, IIGMessage> {

  /**
   * Returns the current minimum value. This is always the last value in the
   * list of BTG ids stored at this vertex. Initially the minimum value is the
   * vertex id.
   *
   * @param vertex The current vertex
   * @return The minimum BTG ID that vertex knows.
   */
  private long getCurrentMinValue(
    Vertex<LongWritable, IIGVertex, NullWritable> vertex) {
    List<Long> btgIDs = vertex.getValue().getBtgIDs();
    return (btgIDs.size() > 0) ? btgIDs.get(
      btgIDs.size() - 1) : vertex.getId().get();
  }

  /**
   * Checks incoming messages for smaller values than the current smallest
   * value.
   *
   * @param messages        All incoming messages
   * @param currentMinValue The current minimum value
   * @return The new (maybe unchanged) minimum value
   */
  private long getNewMinValue(Iterable<IIGMessage> messages,
                              long currentMinValue) {
    long newMinValue = currentMinValue;
    for (IIGMessage message : messages) {
      if (message.getBtgID() < newMinValue) {
        newMinValue = message.getBtgID();
      }
    }
    return newMinValue;
  }

  /**
   * Processes vertices of type Master.
   *
   * @param vertex   The current vertex
   * @param messages All incoming messages
   */
  private void processMasterVertex(
    Vertex<LongWritable, IIGVertex, NullWritable> vertex,
    Iterable<IIGMessage> messages) {
    IIGVertex vertexValue = vertex.getValue();
    for (IIGMessage message : messages) {
      vertexValue.updateNeighbourBtgID(message.getSenderID(),
        message.getBtgID());
    }
    vertexValue.updateBtgIDs();
    // in case the vertex has no neighbours
    if (vertexValue.getBtgIDs().size() == 0) {
      vertexValue.addBtgID(vertex.getId().get());
    }
  }

  /**
   * Processes vertices of type Transactional.
   *
   * @param vertex   The current vertex
   * @param minValue All incoming messages
   */
  private void processTransactionalVertex(
    Vertex<LongWritable, IIGVertex, NullWritable> vertex, long minValue) {
    vertex.getValue().removeLastBtgID();
    vertex.getValue().addBtgID(minValue);
    IIGMessage message = new IIGMessage();
    message.setSenderID(vertex.getId().get());
    message.setBtgID(minValue);
    sendMessageToAllEdges(vertex, message);
  }

  /**
   * The actual BTG computation.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   * @throws java.io.IOException
   */
  @Override
  public void compute(Vertex<LongWritable, IIGVertex, NullWritable> vertex,
                      Iterable<IIGMessage> messages)
    throws IOException {
    if (vertex.getValue().getVertexType() == IIGVertexType.MASTER) {
      processMasterVertex(vertex, messages);
    } else if (vertex.getValue().getVertexType() ==
      IIGVertexType.TRANSACTIONAL) {
      long currentMinValue = getCurrentMinValue(vertex);
      long newMinValue = getNewMinValue(messages, currentMinValue);
      boolean changed = currentMinValue != newMinValue;

      if (getSuperstep() == 0 || changed) {
        processTransactionalVertex(vertex, newMinValue);
      }
    }
    vertex.voteToHalt();
  }
}
