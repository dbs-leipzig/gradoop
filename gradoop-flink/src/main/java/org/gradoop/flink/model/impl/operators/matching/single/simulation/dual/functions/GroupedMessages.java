
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
