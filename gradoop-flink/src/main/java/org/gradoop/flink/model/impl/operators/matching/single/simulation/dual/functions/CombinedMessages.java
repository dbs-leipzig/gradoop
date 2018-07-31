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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.Deletion;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.Message;

/**
 * Combines a collection of deletions to a single message.
 *
 * [deletion] -> message
 *
 * Forwarded fields:
 *
 * f0: recipient id
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CombinedMessages implements
  GroupCombineFunction<Deletion, Message> {

  /**
   * Reduce instantiations
   */
  private final Message reuseMessage = new Message();

  @Override
  public void combine(Iterable<Deletion> deletions,
    Collector<Message> collector) throws Exception {
    reuseMessage.setSenderIds(Lists.newArrayList());
    reuseMessage.setDeletions(Lists.newArrayList());
    reuseMessage.setMessageTypes(Lists.newArrayList());
    boolean first = true;
    for (Deletion deletion : deletions) {
      if (first) {
        reuseMessage.setRecipientId(deletion.getRecipientId());
        first = false;
      }
      reuseMessage.getSenderIds().add(deletion.getSenderId());
      reuseMessage.getDeletions().add(deletion.getDeletion());
      reuseMessage.getMessageTypes().add(deletion.getMessageType());
    }

    collector.collect(reuseMessage);
  }
}
