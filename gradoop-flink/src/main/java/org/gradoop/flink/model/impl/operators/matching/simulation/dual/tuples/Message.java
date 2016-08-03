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
