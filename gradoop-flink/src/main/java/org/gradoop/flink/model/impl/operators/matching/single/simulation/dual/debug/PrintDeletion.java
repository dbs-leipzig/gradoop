
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug;

import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .Deletion;

/**
 * Debug output for {@link Deletion}.
 */
public class PrintDeletion extends Printer<Deletion, GradoopId> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintDeletion.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintDeletion(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Deletion d) {
    return String.format("(%s,%s,%s,%s)",
      vertexMap.get(d.getRecipientId()),
      vertexMap.get(d.getSenderId()),
      d.getDeletion(),
      d.getMessageType());
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
