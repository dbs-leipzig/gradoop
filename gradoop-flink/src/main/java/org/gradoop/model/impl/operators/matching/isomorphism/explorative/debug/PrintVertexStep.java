package org.gradoop.model.impl.operators.matching.isomorphism.explorative.debug;

import org.apache.log4j.Logger;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.VertexStep;

/**
 * Debug output for {@link VertexStep}.
 */
public class PrintVertexStep extends Printer<VertexStep> {

  private static Logger LOG = Logger.getLogger(PrintVertexStep.class);

  public PrintVertexStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(VertexStep vertexStep) {
    return String.format("(%s)", vertexMap.get(vertexStep.getVertexId()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
