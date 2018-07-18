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
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.Message;

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
