
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.tuples.EdgeStep;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;

/**
 * Debug output for {@link EdgeStep}.
 *
 * @param <K> key type
 */
public class PrintEdgeStep<K> extends Printer<EdgeStep<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintEdgeStep.class);

  /**
   * Constructor
   *
   * @param isIterative true, if used in iterative context
   * @param prefix      prefix for debug string
   */
  public PrintEdgeStep(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(EdgeStep<K> edgeStep) {
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
