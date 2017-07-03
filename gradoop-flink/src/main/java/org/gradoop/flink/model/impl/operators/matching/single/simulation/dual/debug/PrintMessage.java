
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .Message;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;

/**
 * Debug output for {@link Message}.
 */
public class PrintMessage extends Printer<Message, GradoopId> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintMessage.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix      prefix for output
   */
  public PrintMessage(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Message m) {
    return String.format("(%s,[%s],[%s],[%s])",
      vertexMap.get(m.getRecipientId()),
      StringUtils.join(convertList(m.getSenderIds(), true), ','),
      StringUtils.join(m.getDeletions(), ','),
      StringUtils.join(m.getMessageTypes(), ','));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
