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
import org.apache.flink.graph.spargel.MessagingFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import static org.gradoop.model.impl.algorithms.epgmlabelpropagation
  .EPGMLabelPropagationAlgorithm.CURRENT_VALUE;

/**
 * Distributes the value of the vertex
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 */
public class LPMessageFunction<
  VD extends EPGMVertex,
  ED extends EPGMEdge>
  extends MessagingFunction<GradoopId, VD, GradoopId, ED> {
  @Override
  public void sendMessages(Vertex<GradoopId, VD> vertex) throws Exception {
    // send current minimum to neighbors
    if (getSuperstepNumber() != 1) {
      sendMessageToAllNeighbors(GradoopId.fromString(
        (String) vertex.getValue().getProperty(CURRENT_VALUE)));
    }
  }
}
