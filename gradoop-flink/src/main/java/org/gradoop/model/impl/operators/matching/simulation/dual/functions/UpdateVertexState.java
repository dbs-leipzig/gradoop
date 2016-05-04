package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.IdPair;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;
import org.gradoop.model.impl.operators.matching.simulation.dual.util.MessageType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Updates the state of a {@link FatVertex} according to the deletions it
 * gets sent.
 *
 * Forwarded Fields First:
 *
 * f0: vertex id
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0")
public class UpdateVertexState
  extends RichJoinFunction<FatVertex, Message, FatVertex> {

  private final String query;

  private QueryHandler queryHandler;

  public UpdateVertexState(String query) {
    this.query = query;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public FatVertex join(FatVertex fatVertex, Message message) throws Exception {

    fatVertex.isUpdated(false);
    if (message != null) {
      fatVertex.isUpdated(true);
      // TODO: message to SELF should be processed first
      for (int i = 0; i < message.getSenderIds().size(); i++) {
        processDeletion(fatVertex, message.getSenderIds().get(i),
          message.getDeletions().get(i), message.getMessageTypes().get(i));
      }
    }
    return fatVertex;
  }

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
    }
  }

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
  private void updateOutgoingEdges(FatVertex fatVertex, Collection<Long>
    queryEdges, GradoopId targetVertex) {
    if (queryEdges != null) {
      Iterator<Map.Entry<IdPair, List<Long>>> edgeIterator =
        fatVertex.getEdgeCandidates().entrySet().iterator();

      while(edgeIterator.hasNext()) {
        Map.Entry<IdPair, List<Long>> e = edgeIterator.next();
        if (targetVertex == null
          || e.getKey().getTargetId().equals(targetVertex)) {
          e.getValue().removeAll(queryEdges);
          if (e.getValue().isEmpty()) {
            edgeIterator.remove();
          }
        }
      }
    }
  }
}
