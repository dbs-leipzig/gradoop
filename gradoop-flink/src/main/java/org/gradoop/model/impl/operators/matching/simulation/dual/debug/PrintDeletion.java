package org.gradoop.model.impl.operators.matching.simulation.dual.debug;

import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Deletion;

/**
 * Debug output for {@link Deletion}.
 */
public class PrintDeletion extends Printer<Deletion> {

  public PrintDeletion(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Deletion d) {
    return String.format("(%d,%s,%s,%s)",
      vertexMap.get(d.getRecipientId()),
      vertexMap.get(d.getSenderId()),
      d.getDeletion(),
      d.getMessageType());
  }
}
