package org.gradoop.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.simulation.dual.util
  .MessageType;

import java.util.List;

/**
 * Bundles a set of {@link Deletion} messages into a single message to update
 * the vertex state accordingly.
 *
 * f0: recipient vertex id
 * f1: sender vertex ids
 * f2: candidate deletions
 * f3: message types
 */
public class Message extends
  Tuple4<GradoopId, List<GradoopId>, List<Long>, List<MessageType>> {

  public GradoopId getRecipientId() {
    return f0;
  }

  public void setRecipientId(GradoopId recipientId) {
    f0 = recipientId;
  }

  public List<GradoopId> getSenderIds() {
    return f1;
  }

  public void setSenderIds(List<GradoopId> senderIds) {
    f1 = senderIds;
  }

  public List<Long> getDeletions() {
    return f2;
  }

  public void setDeletions(List<Long> deletions) {
    f2 = deletions;
  }

  public List<MessageType> getMessageTypes() {
    return f3;
  }

  public void setMessageTypes(List<MessageType> messageTypes) {
    f3 = messageTypes;
  }
}
