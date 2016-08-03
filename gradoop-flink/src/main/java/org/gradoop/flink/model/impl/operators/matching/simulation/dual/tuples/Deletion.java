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

package org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.util.MessageType;

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
