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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.simulation.dual.tuples.Message;

/**
 * Combines a collection of messages to a single message.
 *
 * [message] -> message
 *
 * Forwarded fields:
 *
 * f0: recipient id
 */
public class GroupedMessages implements
  GroupReduceFunction<Message, Message> {

  @Override
  public void reduce(Iterable<Message> messages,
    Collector<Message> collector) throws Exception {
    boolean first = true;
    Message result = null;
    for (Message message : messages) {
      if (first) {
        result = message;
        first = false;
      } else {
        result.getSenderIds().addAll(message.getSenderIds());
        result.getDeletions().addAll(message.getDeletions());
        result.getMessageTypes().addAll(message.getMessageTypes());
      }
    }
    collector.collect(result);
  }
}
