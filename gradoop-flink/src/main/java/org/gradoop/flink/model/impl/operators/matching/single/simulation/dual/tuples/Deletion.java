/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
