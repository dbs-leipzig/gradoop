package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Deletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.FatVertex;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.IdPair;
import org.gradoop.model.impl.operators.matching.simulation.dual.util.MessageType;
import org.s1ck.gdl.model.Edge;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Validates the neighborhood of a {@link FatVertex} according to the query.
 *
 * For each query vertex candidate, the flatMap function checks if the vertex
 * has the corresponding incident incoming and outgoing edges. If this is not
 * the case, the vertex sends delete messages to all of its neighbors.
 */
public class ValidateNeighborhood
  extends RichFlatMapFunction<FatVertex, Deletion> {

  private final String query;

  private final Deletion reuseDeletion;

  private QueryHandler queryHandler;

  public ValidateNeighborhood(String query) {
    this.query          = query;
    this.reuseDeletion  = new Deletion();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void flatMap(FatVertex fatVertex, Collector<Deletion> collector)
    throws Exception {

    List<Long> deletions = Lists
      .newArrayListWithCapacity(fatVertex.getCandidates().size());
    Set<Long> outgoingEdgeCandidates = getOutgoingEdgeCandidates(fatVertex);

    for (Long vQ : fatVertex.getCandidates()) {
      if (!isValidCandidate(vQ, fatVertex, outgoingEdgeCandidates)) {
        deletions.add(vQ);
      }
    }

    if (!deletions.isEmpty()) {
      sendDeletions(deletions, fatVertex, collector);
    }
  }

  /**
   * Validates if the given query vertex candidate is a valid candidate for the
   * vertex.
   *
   * @param vQ vertex candidate
   * @param fatVertex vertex
   * @param outgoingEdgeCandidates outgoing edge candidates
   * @return true, if {@code fatVertex} is a valid embedding for {@code vQ}
   */
  private boolean isValidCandidate(Long vQ, FatVertex fatVertex,
    Set<Long> outgoingEdgeCandidates) {
    boolean isValidChild = isValidChild(vQ, fatVertex);

    boolean isValidParent = true;
    if (isValidChild) {
      isValidParent = isValidParent(vQ, outgoingEdgeCandidates);
    }
    return isValidChild && isValidParent;
  }

  /**
   * Validate incoming edge candidates relating to the given vertex candidate.
   *
   * @param vQ vertex candidate
   * @param fatVertex vertex
   * @return true, if {@code fatVertex} is a valid child for {@code vQ}
   */
  private boolean isValidChild(Long vQ, FatVertex fatVertex) {
    boolean isValidChild = true;
    Collection<Edge> inE = queryHandler.getEdgesByTargetVertexId(vQ);
    if (inE != null) {
      for (Edge eQIn : inE) {
        if (fatVertex.getIncomingCandidateCounts()[(int) eQIn.getId()] == 0) {
          isValidChild = false;
          break;
        }
      }
    }
    return isValidChild;
  }

  /**
   * Validate outgoing edge candidates relating to the given vertex candidate.
   *
   * @param vQ vertex candidate
   * @param outgoingEdgeCandidates outgoing edge candidates
   * @return true, if {@code fatVertex} is a valid parent for {@code vQ}
   */
  private boolean isValidParent(Long vQ, Set<Long> outgoingEdgeCandidates) {
    boolean isValidParent = true;
    Collection<Edge> outE = queryHandler.getEdgesBySourceVertexId(vQ);
    if (outE != null) {
      for (Edge eQOut : outE) {
        if (!outgoingEdgeCandidates.contains(eQOut.getId())) {
          isValidParent = false;
          break;
        }
      }
    }
    return isValidParent;
  }

  /**
   * Sends delete messages to the vertex' neighborhood including itself.
   *
   * @param deletions vertex candidates that need to be deleted
   * @param fatVertex current vertex
   * @param collector message collector
   */
  private void sendDeletions(List<Long> deletions, FatVertex fatVertex,
    Collector<Deletion> collector) {
    reuseDeletion.setSenderId(fatVertex.getVertexId());
    boolean toBeRemoved = deletions.size() == fatVertex.getCandidates().size();
    for (Long deletion : deletions) {
      reuseDeletion.setDeletion(deletion);
      sendToSelf(fatVertex, collector);
      sendToParents(fatVertex, collector, toBeRemoved);
      sendToChildren(fatVertex, collector, toBeRemoved);
    }
  }

  /**
   * Sends delete message to itself.
   *
   * @param fatVertex fat vertex
   * @param collector message collector
   */
  private void sendToSelf(FatVertex fatVertex, Collector<Deletion> collector) {
    reuseDeletion.setMessageType(MessageType.FROM_SELF);
    reuseDeletion.setRecipientId(fatVertex.getVertexId());
    collector.collect(reuseDeletion);
  }

  /**
   * Sends delete messages to all parents.
   *
   * @param fatVertex   fat vertex
   * @param collector   message collector
   * @param toBeRemoved true, if {@code fatVertex} will be removed
   */
  private void sendToParents(FatVertex fatVertex, Collector<Deletion> collector,
    boolean toBeRemoved) {
    reuseDeletion.setMessageType(toBeRemoved ?
      MessageType.FROM_CHILD_REMOVE : MessageType.FROM_CHILD);

    for (GradoopId gradoopId : fatVertex.getParentIds()) {
      reuseDeletion.setRecipientId(gradoopId);
      collector.collect(reuseDeletion);
    }
  }

  /**
   * Sends delete messages to all children.
   *
   * @param fatVertex   fat vertex
   * @param collector   message collector
   * @param toBeRemoved true, if {@code fatVertex} will be removed
   */
  private void sendToChildren(FatVertex fatVertex,
    Collector<Deletion> collector, boolean toBeRemoved) {
    reuseDeletion.setMessageType(toBeRemoved ?
      MessageType.FROM_PARENT_REMOVE : MessageType.FROM_PARENT);
    for (IdPair idPair : fatVertex.getEdgeCandidates().keySet()) {
      reuseDeletion.setRecipientId(idPair.getTargetId());
      collector.collect(reuseDeletion);
    }
  }

  /**
   * Returns all outgoing edge candidates for the given vertex.
   *
   * @param fatVertex fat vertex
   * @return all outgoing edge candidates of {@code fatVertex}
   */
  private Set<Long> getOutgoingEdgeCandidates(FatVertex fatVertex) {
      Set<Long> outgoingEdgeCandidates = Sets.newHashSet();
      for (List<Long> candidates : fatVertex.getEdgeCandidates().values()) {
        outgoingEdgeCandidates.addAll(candidates);
      }
      return outgoingEdgeCandidates;
  }
}
