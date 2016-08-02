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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples
  .IdPair;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples
  .Message;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.FatVertex;

import org.gradoop.flink.model.impl.operators.matching.simulation.dual.util.MessageType;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Updates the state of a {@link FatVertex} according to the message it
 * receives.
 *
 * Forwarded Fields First:
 *
 * f0: vertex id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
public class UpdateVertexState
  extends RichJoinFunction<FatVertex, Message, FatVertex> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * GDL query
   */
  private final String query;

  /**
   * Query handler
   */
  private transient QueryHandler queryHandler;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public UpdateVertexState(String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = new QueryHandler(query);
  }

  @Override
  public FatVertex join(FatVertex fatVertex, Message message) throws Exception {
    if (message != null) {
      // TODO: message to SELF should be processed first
      for (int i = 0; i < message.getSenderIds().size(); i++) {
        processDeletion(fatVertex, message.getSenderIds().get(i),
          message.getDeletions().get(i), message.getMessageTypes().get(i));
      }
      fatVertex.setUpdated(true);
    } else {
      fatVertex.setUpdated(false);
    }

    return fatVertex;
  }

  /**
   * Processes a deletion on the current vertex.
   *
   * @param fatVertex   fat vertex
   * @param senderId    sender vertexId
   * @param deletion    sender vertex candidate deletion id
   * @param messageType message type
   */
  private void processDeletion(FatVertex fatVertex, GradoopId senderId,
    Long deletion, MessageType messageType) {
    switch (messageType) {
    case FROM_SELF:
      updateCandidates(fatVertex, deletion);
      break;
    case FROM_CHILD:
      updateOutgoingEdges(fatVertex,
        queryHandler.getEdgeIdsByTargetVertexId(deletion), senderId);
      break;
    case FROM_PARENT:
      updateIncomingEdges(fatVertex,
        queryHandler.getEdgeIdsBySourceVertexId(deletion));
      break;
    case FROM_CHILD_REMOVE:
      updateOutgoingEdges(fatVertex, senderId);
      break;
    case FROM_PARENT_REMOVE:
      updateIncomingEdges(fatVertex,
        queryHandler.getEdgeIdsBySourceVertexId(deletion));
      updateParentIds(fatVertex, senderId);
      break;
    default:
      throw new IllegalArgumentException("Unsupported type: " + messageType);
    }
  }

  /**
   * Removes the sender vertex id from the parents of the current vertex.
   *
   * @param fatVertex fat vertex
   * @param senderId  sender vertex id
   */
  private void updateParentIds(FatVertex fatVertex, GradoopId senderId) {
    fatVertex.getParentIds().remove(senderId);
  }

  /**
   * Updates the candidates of the given vertex. If the vertex has still
   * candidates left, the outgoing edges will be updated accordingly.
   *
   * @param fatVertex fat vertex
   * @param deletion  vertex candidate to be removed
   */
  private void updateCandidates(FatVertex fatVertex, Long deletion) {
    fatVertex.getCandidates().remove(deletion);
    if (!fatVertex.getCandidates().isEmpty()) {
      updateOutgoingEdges(fatVertex,
        queryHandler.getEdgeIdsBySourceVertexId(deletion));
    }
  }

  /**
   * Updates incoming edge of the given vertex. The method decrements the
   * counter for each query edge candidate.
   *
   * @param fatVertex   fat vertex
   * @param queryEdges  edge candidates to be removed
   */
  private void updateIncomingEdges(FatVertex fatVertex, Collection<Long>
    queryEdges) {
    for (Long eQ : queryEdges) {
      if (fatVertex.getIncomingCandidateCounts()[eQ.intValue()] > 0) {
        fatVertex.getIncomingCandidateCounts()[eQ.intValue()]--;
      }
    }
  }

  /**
   * Deletes all outgoing edges which point to the given target id.
   *
   * @param vertex    fat vertex
   * @param targetId  target vertex
   */
  private void updateOutgoingEdges(FatVertex vertex, GradoopId targetId) {
    Iterator<IdPair> iterator = vertex.getEdgeCandidates().keySet().iterator();
    while (iterator.hasNext()) {
      if (iterator.next().getTargetId().equals(targetId)) {
        iterator.remove();
      }
    }
  }

  /**
   * Updates the outgoing edges of the given vertex according to a collection
   * of edge candidates which will be deleted.
   *
   * @param fatVertex   fat vertex
   * @param queryEdges  edge candidates to be removed
   */
  private void updateOutgoingEdges(FatVertex fatVertex,
    Collection<Long> queryEdges) {
    updateOutgoingEdges(fatVertex, queryEdges, null);
  }

  /**
   * Updates the outgoing edges of the given vertex according to a collection of
   * edge candidates which will be deleted. Updates only those outgoing edges
   * which point to a specific target vertex.
   *
   * @param fatVertex     fat vertex
   * @param queryEdges    edge candidates to be removed
   * @param targetVertex  target vertex to consider
   */
  private void updateOutgoingEdges(FatVertex fatVertex,
    Collection<Long> queryEdges, GradoopId targetVertex) {
    if (queryEdges != null) {
      Iterator<Map.Entry<IdPair, boolean[]>> edgeIterator = fatVertex
        .getEdgeCandidates().entrySet().iterator();
      while (edgeIterator.hasNext()) {
        Map.Entry<IdPair, boolean[]> e = edgeIterator.next();
        if (targetVertex == null ||
          e.getKey().getTargetId().equals(targetVertex)) {
          // remove edge candidates for that edge
          for (Long eQ : queryEdges) {
            e.getValue()[eQ.intValue()] = false;
          }
          // remove edge if there are no candidates left
          if (allFalse(e.getValue())) {
            edgeIterator.remove();
          }
        }
      }
    }
  }

  /**
   * Checks if the given array contains only false values.
   *
   * @param a array
   * @return true, iff all values are false
   */
  private boolean allFalse(boolean[] a) {
    for (boolean b : a) {
      if (b) {
        return false;
      }
    }
    return true;
  }
}
