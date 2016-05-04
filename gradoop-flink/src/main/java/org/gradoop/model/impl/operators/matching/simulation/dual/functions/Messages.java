package org.gradoop.model.impl.operators.matching.simulation.dual.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Deletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;
import org.gradoop.model.impl.operators.matching.simulation.dual.util.MessageType;

/**
 * Combines a collection of {@link Deletion} to a single message and
 * groups messages with the same recipient.
 */
public class Messages implements
  GroupCombineFunction<Deletion, Message>,
  GroupReduceFunction<Message, Message> {

  private final Message reuseMessage;

  public Messages() {
    reuseMessage = new Message();
  }

  @Override
  public void combine(Iterable<Deletion> deletions,
    Collector<Message> collector) throws Exception {
    reuseMessage.setSenderIds(Lists.<GradoopId>newArrayList());
    reuseMessage.setDeletions(Lists.<Long>newArrayList());
    reuseMessage.setMessageTypes(Lists.<MessageType>newArrayList());
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
