package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;


/**
 * Debug output for {@link Message}.
 */
public class PrintMessage extends Printer<Message> {

  public PrintMessage(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Message m) {
    return String.format("(%d,[%s],[%s],[%s])",
      vertexMap.get(m.getRecipientId()),
      StringUtils.join(convertList(m.getSenderIds(), true), ','),
      StringUtils.join(m.getDeletions(), ','),
      StringUtils.join(m.getMessageTypes(), ','));
  }
}
