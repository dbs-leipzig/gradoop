package org.gradoop.model.impl.operators.matching.isomorphism.naive.debug;

import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.isomorphism.naive.tuples.EdgeStep;

/**
 * Debug output for {@link EdgeStep}.
 */
public class PrintEdgeStep extends Printer<EdgeStep> {

  private static final Logger LOG = Logger.getLogger(PrintEdgeStep.class);

  public PrintEdgeStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(EdgeStep edgeStep) {
    return String.format("(%s,%s,%s)",
      edgeMap.get(edgeStep.getEdgeId()),
      vertexMap.get(edgeStep.getTiePoint()),
      vertexMap.get(edgeStep.getNextId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
