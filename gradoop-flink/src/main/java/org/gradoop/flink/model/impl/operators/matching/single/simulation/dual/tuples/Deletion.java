
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.util.MessageType;

/**
 * Represents a delete information sent by a vertex to another vertex.
 *
 * f0: recipient vertex id
 * f1: sender vertex id
 * f2: candidate for deletion
 * f3: message type
 */
public class Deletion extends Tuple4<GradoopId, GradoopId, Long, MessageType> {

  public GradoopId getRecipientId() {
    return f0;
  }

  public void setRecipientId(GradoopId recipientId) {
    f0 = recipientId;
  }

  public GradoopId getSenderId() {
    return f1;
  }

  public void setSenderId(GradoopId senderId) {
    f1 = senderId;
  }

  public Long getDeletion() {
    return f2;
  }

  public void setDeletion(Long deletion) {
    f2 = deletion;
  }

  public MessageType getMessageType() {
    return f3;
  }

  public void setMessageType(MessageType messageType) {
    f3 = messageType;
  }
}
